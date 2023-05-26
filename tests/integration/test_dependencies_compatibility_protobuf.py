"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.protobuf.kotlin_wrapper import trim_margin
from tests.utils import create_subject_name_factory

import pytest


@pytest.mark.parametrize("trail", ["", "/"])
async def test_protobuf_schema_compatibility(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()
    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    original_dependencies = """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message Hint {
            |        string hint_str = 1;
            |    }
            |}
            |"""

    evolved_dependencies = """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message Hint {
            |        string hint_str = 1;
            |    }
            |}
            |"""

    original_dependencies = trim_margin(original_dependencies)
    res = await registry_async_client.post(
        "subjects/container1/versions", json={"schemaType": "PROTOBUF", "schema": original_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_dependencies = trim_margin(evolved_dependencies)
    res = await registry_async_client.post(
        "subjects/container2/versions", json={"schemaType": "PROTOBUF", "schema": evolved_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "container1.proto";
            |message TestMessage {
            |    message Value {
            |        .a1.container.Hint hint = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""

    original_schema = trim_margin(original_schema)

    original_references = [{"name": "container1.proto", "subject": "container1", "version": 1}]
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": original_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = """
            |syntax = "proto3";
            |package a1;
            |import "container2.proto";
            |message TestMessage {
            |    message Value {
            |        .a1.container.Hint hint = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""
    evolved_schema = trim_margin(evolved_schema)
    evolved_references = [{"name": "container2.proto", "subject": "container2", "version": 1}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema, "references": evolved_references},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": True}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_protobuf_schema_compatibility_dependencies(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    original_dependencies = """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message Hint {
            |        string hint_str = 1;
            |    }
            |}
            |"""

    evolved_dependencies = """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message Hint {
            |        int32 hint_str = 1;
            |    }
            |}
            |"""

    original_dependencies = trim_margin(original_dependencies)
    res = await registry_async_client.post(
        "subjects/container1/versions", json={"schemaType": "PROTOBUF", "schema": original_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_dependencies = trim_margin(evolved_dependencies)
    res = await registry_async_client.post(
        "subjects/container2/versions", json={"schemaType": "PROTOBUF", "schema": evolved_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "container1.proto";
            |message TestMessage {
            |    message Value {
            |        .a1.container.Hint hint = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""

    original_schema = trim_margin(original_schema)

    original_references = [{"name": "container1.proto", "subject": "container1", "version": 1}]
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": original_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = """
            |syntax = "proto3";
            |package a1;
            |import "container2.proto";
            |message TestMessage {
            |    message Value {
            |        .a1.container.Hint hint = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""
    evolved_schema = trim_margin(evolved_schema)
    evolved_references = [{"name": "container2.proto", "subject": "container2", "version": 1}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema, "references": evolved_references},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_protobuf_schema_compatibility_dependencies1(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    original_dependencies = """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message H {
            |        string s = 1;
            |    }
            |}
            |"""

    evolved_dependencies = """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message H {
            |        int32 s = 1;
            |    }
            |}
            |"""

    original_dependencies = trim_margin(original_dependencies)
    res = await registry_async_client.post(
        "subjects/container1/versions", json={"schemaType": "PROTOBUF", "schema": original_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_dependencies = trim_margin(evolved_dependencies)
    res = await registry_async_client.post(
        "subjects/container2/versions", json={"schemaType": "PROTOBUF", "schema": evolved_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "container1.proto";
            |message TestMessage {
            |    message V {
            |        .a1.container.H h = 1;
            |        int32 x = 2;
            |    }
            |    string t = 1;
            |    .a1.TestMessage.V v = 2;
            |}
            |"""

    original_schema = trim_margin(original_schema)

    original_references = [{"name": "container1.proto", "subject": "container1", "version": 1}]
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": original_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = """
            |syntax = "proto3";
            |package a1;
            |import "container2.proto";
            |message TestMessage {
            |    message V {
            |        .a1.container.H h = 1;
            |        int32 x = 2;
            |    }
            |    string t = 1;
            |    .a1.TestMessage.V v = 2;
            |}
            |"""
    evolved_schema = trim_margin(evolved_schema)
    evolved_references = [{"name": "container2.proto", "subject": "container2", "version": 1}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema, "references": evolved_references},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False}


# Do compatibility check when message field is altered from referenced type to google type
async def test_protobuf_schema_compatibility_dependencies1g(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_schema_compatibility_dep1g")()
    subject_container = create_subject_name_factory("test_protobuf_schema_compatibility_dep1g_container")()

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    container = """
syntax = "proto3";
package a1;
message container {
    message H {
        string s = 1;
    }
}
"""

    res = await registry_async_client.post(
        f"subjects/{subject_container}/versions", json={"schemaType": "PROTOBUF", "schema": container}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = """
syntax = "proto3";
package a1;
import "container.proto";
message TestMessage {
    message V {
        .a1.container.H h = 1;
        int32 x = 2;
    }
    string t = 1;
    .a1.TestMessage.V v = 2;
}
"""

    original_references = [{"name": "container.proto", "subject": subject_container, "version": 1}]
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": original_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = """
syntax = "proto3";
package a1;
import "google/type/postal_address.proto";
message TestMessage {
    message V {
        google.type.PostalAddress h = 1;
        int32 x = 2;
    }
    string t = 1;
    .a1.TestMessage.V v = 2;
}
"""

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False}


# Do compatibility check when message field is altered from google type to referenced type
async def test_protobuf_schema_compatibility_dependencies1g_otherway(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_schema_compatibility_dep1g_back")()
    subject_container = create_subject_name_factory("test_protobuf_schema_compatibility_dep1g_back_container")()

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    container = """
syntax = "proto3";
package a1;
message container {
    message H {
        string s = 1;
    }
}
"""

    res = await registry_async_client.post(
        f"subjects/{subject_container}/versions", json={"schemaType": "PROTOBUF", "schema": container}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = """
syntax = "proto3";
package a1;
import "google/type/postal_address.proto";
message TestMessage {
    message V {
        google.type.PostalAddress h = 1;
        int32 x = 2;
    }
    string t = 1;
    .a1.TestMessage.V v = 2;
}
"""

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "PROTOBUF", "schema": original_schema},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = """
syntax = "proto3";
package a1;
import "container.proto";
message TestMessage {
    message V {
        .a1.container.H h = 1;
        int32 x = 2;
    }
    string t = 1;
    .a1.TestMessage.V v = 2;
}
"""

    container_references = [{"name": "container.proto", "subject": subject_container, "version": 1}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema, "references": container_references},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_protobuf_schema_compatibility_dependencies2(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    original_dependencies = """
            |syntax = "proto3";
            |message container {
            |    message H {
            |        string s = 1;
            |    }
            |}
            |"""

    evolved_dependencies = """
            |syntax = "proto3";
            |message container {
            |    message H {
            |        int32 s = 1;
            |    }
            |}
            |"""

    original_dependencies = trim_margin(original_dependencies)
    res = await registry_async_client.post(
        "subjects/container1/versions", json={"schemaType": "PROTOBUF", "schema": original_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_dependencies = trim_margin(evolved_dependencies)
    res = await registry_async_client.post(
        "subjects/container2/versions", json={"schemaType": "PROTOBUF", "schema": evolved_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = """
            |syntax = "proto3";
            |import "container1.proto";
            |message TestMessage {
            |    message V {
            |        .container.H h = 1;
            |        int32 x = 2;
            |    }
            |    string t = 1;
            |    .TestMessage.V v = 2;
            |}
            |"""

    original_schema = trim_margin(original_schema)

    original_references = [{"name": "container1.proto", "subject": "container1", "version": 1}]
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": original_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = """
            |syntax = "proto3";
            |import "container2.proto";
            |message TestMessage {
            |    message V {
            |        .container.H h = 1;
            |        int32 x = 2;
            |    }
            |    string t = 1;
            |    .TestMessage.V v = 2;
            |}
            |"""
    evolved_schema = trim_margin(evolved_schema)
    evolved_references = [{"name": "container2.proto", "subject": "container2", "version": 1}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema, "references": evolved_references},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False}


SIMPLE_SCHEMA = """\
syntax = "proto3";

message Msg {
  string name = 1;
}
"""


async def test_protobuf_schema_references_rejected_values(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_schema_references_values")()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    res = await registry_async_client.post(
        f"subjects/{subject}/versions", json={"schemaType": "PROTOBUF", "schema": SIMPLE_SCHEMA, "references": 1}
    )
    assert res.status_code == 400

    res = await registry_async_client.post(
        f"subjects/{subject}/versions", json={"schemaType": "PROTOBUF", "schema": SIMPLE_SCHEMA, "references": "foo"}
    )
    assert res.status_code == 400

    res = await registry_async_client.post(
        f"subjects/{subject}/versions", json={"schemaType": "PROTOBUF", "schema": SIMPLE_SCHEMA, "references": False}
    )
    assert res.status_code == 400

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "PROTOBUF", "schema": SIMPLE_SCHEMA, "references": {"this_is_object": True}},
    )
    assert res.status_code == 400


async def test_protobuf_schema_references_valid_values(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_schema_references_values")()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    # null value accepted for compatibility, same as empty list
    res = await registry_async_client.post(
        f"subjects/{subject}/versions", json={"schemaType": "PROTOBUF", "schema": SIMPLE_SCHEMA, "references": None}
    )
    assert res.status_code == 200

    res = await registry_async_client.post(
        f"subjects/{subject}/versions", json={"schemaType": "PROTOBUF", "schema": SIMPLE_SCHEMA, "references": []}
    )
    assert res.status_code == 200


async def test_protobuf_references_latest(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_references_latest")()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    original_dependencies = trim_margin(
        """
            |syntax = "proto3";
            |package a1;
            |message container {
            |    message Hint {
            |        string hint_str = 1;
            |    }
            |}
            |"""
    )

    res = await registry_async_client.post(
        f"subjects/{subject}_base/versions", json={"schemaType": "PROTOBUF", "schema": original_dependencies}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    original_schema = trim_margin(
        """
            |syntax = "proto3";
            |package a1;
            |import "container1.proto";
            |message TestMessage {
            |    message Value {
            |        .a1.container.Hint hint = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""
    )

    original_references = [{"name": "container1.proto", "subject": f"{subject}_base", "version": -1}]
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": original_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()


async def test_protobuf_customer_update_when_having_references(registry_async_client: Client) -> None:
    subject_place = create_subject_name_factory("test_protobuf_place")()
    subject_customer = create_subject_name_factory("test_protobuf_customer")()

    place_proto = """\
syntax = "proto3";
package a1;
message Place {
        string city = 1;
        int32 zone = 2;
}
"""

    body = {"schemaType": "PROTOBUF", "schema": place_proto}
    res = await registry_async_client.post(f"subjects/{subject_place}/versions", json=body)

    assert res.status_code == 200

    customer_proto = """\
syntax = "proto3";
package a1;
import "place.proto";
import "google/type/postal_address.proto";
// @producer: another comment
message Customer {
        string name = 1;
        int32 code = 2;
        Place place = 3;
        google.type.PostalAddress address = 4;
}
"""
    body = {
        "schemaType": "PROTOBUF",
        "schema": customer_proto,
        "references": [
            {
                "name": "place.proto",
                "subject": subject_place,
                "version": -1,
            }
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject_customer}/versions", json=body)

    assert res.status_code == 200

    customer_proto_updated = """\
syntax = "proto3";
package a1;
import "place.proto";
import "google/type/postal_address.proto";
// @consumer: the comment was incorrect, updating it now
message Customer {
        string name = 1;
        int32 code = 2;
        Place place = 3;
        google.type.PostalAddress address = 4;
}
"""

    body = {
        "schemaType": "PROTOBUF",
        "schema": customer_proto_updated,
        "references": [
            {
                "name": "place.proto",
                "subject": subject_place,
                "version": -1,
            }
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject_customer}/versions", json=body)

    assert res.status_code == 200
