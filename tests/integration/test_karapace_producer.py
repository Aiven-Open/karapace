from karapace.core.messaging import KarapaceProducer
from karapace.core.kafka.producer import KafkaProducer
from aiokafka.errors import KafkaUnavailableError
from concurrent.futures import Future

from unittest import mock
import pytest

def test_schema_registry_producer_re_inits_on_resolve_error(
    karapace_producer: KarapaceProducer
) -> None:
    karapace_producer.initialize_karapace_producer()

    with mock.patch(f"{KafkaProducer.__module__}.{KafkaProducer.__qualname__}.send") as send_mock:
            future = Future()
            future.set_exception(KafkaUnavailableError("Failed to resolve 'xyz'"))
            send_mock.return_value = future

            with pytest.raises(KafkaUnavailableError):
                karapace_producer.send_message(
                    key=dict(keytype="json", subject="a", version="1", magic="a"), 
                    value=dict(test="b")
                )
    
            assert send_mock.call_count == 2
