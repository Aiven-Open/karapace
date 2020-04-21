from kafka import KafkaProducer, KafkaAdminClient
from karapace.rapu import HTTPResponse
import copy


def test_internal(consumer_manager):
    # starting this more as a sanity check, and will probably end up with testing the _assert_* methods here
    consumer_manager, config_path = consumer_manager
    create_payload = {
        "format": "avro",
        "auto.offset.reset": "latest",
        "fetch.min.bytes": 10000,
        "auto.commit.enable": "true"
    }
    group_name = "test_group"
    resp = None
    try:
        consumer_manager.create_consumer(group_name=group_name, request_data=create_payload, content_type=None)
    except HTTPResponse as e:
        resp = e
    assert resp is not None
    assert resp.ok()
    body = resp.body
    # valid base uri
    assert "base_uri" in body
    assert body["base_uri"] == consumer_manager.hostname
    # add with the same name fails
    with_name = copy.copy(create_payload)
    with_name["name"] = body["instance_id"]
    try:
        consumer_manager.create_consumer(group_name=group_name, request_data=with_name, content_type=None)
    except HTTPResponse as e:
        resp = e
    assert not resp.ok()
    assert resp.status == 409, f"Expected conflict but got a different error: {resp.body}"
    invalid_fetch = copy.copy(create_payload)
    invalid_fetch["fetch.min.bytes"] = -1
    try:
        consumer_manager.create_consumer(group_name=group_name, request_data=invalid_fetch, content_type=None)
    except HTTPResponse as e:
        resp = e
    assert not resp.ok()
    assert resp.status == 422, f"Expected invalid fetch request value config for: {resp.body}"
