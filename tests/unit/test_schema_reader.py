from concurrent.futures import ThreadPoolExecutor
from karapace.schema_reader import OffsetsWatcher

import random
import time


def test_offset_watcher() -> None:
    watcher = OffsetsWatcher()
    timeout = 0.5

    # A largish number of iteration useful to stress the code
    total_number_of_offsets = 100

    # A random sleep is added after every iteration of both the consumer and
    # the producer, the goal is to simulate race conditions were the producer
    # will see an event first,  even though the consumer is more likely of
    # doing so.
    max_sleep = 0.01

    assert timeout > max_sleep, "Bad configuration, test will fail."
    assert max_sleep * total_number_of_offsets < 5, "Bad configuration, test would be too slow."

    def consume() -> None:
        for offset in range(total_number_of_offsets):
            assert watcher.wait_for_offset(expected_offset=offset, timeout=timeout), "Event must be produced."
            sleep = random.uniform(0, max_sleep)
            time.sleep(sleep)

    def produce() -> None:
        for offset in range(total_number_of_offsets):
            watcher.offset_seen(new_offset=offset)
            sleep = random.uniform(0, max_sleep)
            time.sleep(sleep)

    with ThreadPoolExecutor(max_workers=2) as executor:
        consumer = executor.submit(consume)
        producer = executor.submit(produce)
        assert consumer.result() is None, "Thread should finish without errors"
        assert producer.result() is None, "Thread should finish without errors"

    assert (
        len(watcher._events) == 0  # pylint: disable=protected-access
    ), "all events have been consumed, so the mapping must be empty"
