"""
karapace - metrics
Supports collection of system metrics
list of supported metrics:
connections-active - The number of active HTTP(S) connections to server.
                     Data collected inside aiohttp request handler.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
import time
from typing import List, Union

from karapace.statsd import StatsClient



class Sample:
    def __init__(self, value: float, now: float):
        self.value = value
        self.default_value = value
        self.event_number = 0
        self.start_time = now

    def reset(self, start_time: float) -> None:
        self.start_time = start_time
        self.value = self.default_value
        self.event_number = 0

    def event(self) -> None:
        self.event_number += 1

    def is_finished(self, time_window: float) -> bool:
        if time.monotonic() - self.start_time > time_window:
            return True
        return False


class DraftStats:
    def __init__(self, time_window: float):
        self.samples: List[Sample] = []
        self.max_samples = 2
        self.current_sample = 0
        self.time_window = time_window

    def record(self, value: float, record_time: float) -> None:
        if len(self.samples) == 0:
            self.samples.append(Sample(0, record_time))
        if self.samples[self.current_sample].is_finished(self.time_window):
            self.current_sample = (self.current_sample + 1) % self.max_samples
            if len(self.samples) <= self.current_sample:
                self.samples.append(Sample(0.0, record_time))

            else:
                self.samples[self.current_sample].reset(record_time)

        self.commit(self.samples[self.current_sample], value)
        self.samples[self.current_sample].event()

    def commit(self, sample: Sample, value: float) -> None:
        pass

    def result(self) -> float:
        return 0.0


class Avg(DraftStats):
    def __init__(self, time_window: float):
        super().__init__(time_window)

    def commit(self, sample: Sample, value: float) -> None:
        sample.value += value

    def result(self) -> float:
        total = 0.0
        event_number = 0
        for s in self.samples:
            total += s.value
            event_number += s.event_number

        if event_number == 0:
            return 0
        return total / event_number


class Max(DraftStats):
    def __init__(self, time_window: float):
        super().__init__(time_window)

    def commit(self, sample: Sample, value: float) -> None:
        sample.value = max(value, sample.value)

    def result(self) -> float:
        maximum = 0.0
        for s in self.samples:
            maximum = max(s.value, maximum)
        return maximum


class BasicSensor:
    def __init__(self, name: str):
        self._name = name

    def report(self, stats: StatsClient) -> None:
        pass


class CounterSensor(BasicSensor):
    def __init__(self, name: str, value: float) -> None:
        super().__init__(name)
        self._value = value

    def record(self, value: float) -> None:
        self._value += value

    def report(self, stats_client: StatsClient) -> None:
        stats_client.gauge(self._name, self._value)


class SampledSensor(BasicSensor):

    def __init__(self, name: str, stats: DraftStats) -> None:
        super().__init__(name)
        self.stats = stats

    def record(self, value: float, record_time: float) -> None:
        self.stats.record(value, record_time)

    def report(self, stats_client: StatsClient) -> None:
        stats_client.gauge(self._name, self.stats.result())


class Metrics:
    def __init__(self, stats_client: StatsClient, prefix: str, time_window: float) -> None:
        self.stats_client = stats_client
        """ app_connections_metric will gauge active connections at present moment """

        self.sensors: List[Union[CounterSensor, SampledSensor]] = []
        self.connections_sensor = CounterSensor(f"{prefix}_connections_active", 0.0)
        self.request_size_max_sensor = SampledSensor(f"{prefix}_request_size_max", Max(time_window))
        self.request_size_avg_sensor = SampledSensor(f"{prefix}_request_size_avg", Avg(time_window))
        self.response_size_max_sensor = SampledSensor(f"{prefix}_response_size_max", Max(time_window))
        self.response_size_avg_sensor = SampledSensor(f"{prefix}_response_size_avg", Avg(time_window))
        self.sensors.append(self.connections_sensor)
        self.sensors.append(self.request_size_max_sensor)
        self.sensors.append(self.request_size_avg_sensor)
        self.sensors.append(self.response_size_avg_sensor)
        self.sensors.append(self.response_size_max_sensor)


    def connection(self) -> None:
        self.connections_sensor.record(1.0)

    def request(self, size: int) -> None:
        t = time.monotonic()
        self.request_size_max_sensor.record(size, t)
        self.request_size_avg_sensor.record(size, t)

    def response(self, size: int) -> None:
        self.connections_sensor.record(-1.0)
        t = time.monotonic()
        self.response_size_max_sensor.record(size, t)
        self.response_size_avg_sensor.record(size, t)

    def report(self) -> None:
        for s in self.sensors:
            s.report(self.stats_client)

    def cleanup(self) -> None:
        self.report()
