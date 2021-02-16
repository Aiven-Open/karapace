from karapace.rapu import HTTPRequest, REST_ACCEPT_RE, REST_CONTENT_TYPE_RE


async def test_header_get():
    req = HTTPRequest(url="", query="", headers={}, path_for_stats="", method="GET")
    assert "Content-Type" not in req.headers
    for v in ["Content-Type", "content-type", "CONTENT-tYPE", "coNTENT-TYpe"]:
        assert req.get_header(v) == "application/json"


def test_rest_accept_re():
    # incomplete headers
    assert not REST_ACCEPT_RE.match('')
    assert not REST_ACCEPT_RE.match('application/')
    assert not REST_ACCEPT_RE.match('application/vnd.kafka')
    assert not REST_ACCEPT_RE.match('application/vnd.kafka.json')
    # Unsupported serialization formats
    assert not REST_ACCEPT_RE.match('application/vnd.kafka+avro')
    assert not REST_ACCEPT_RE.match('application/vnd.kafka+protobuf')
    assert not REST_ACCEPT_RE.match('application/vnd.kafka+binary')

    assert REST_ACCEPT_RE.match('application/json').groupdict() == {
        'embedded_format': None,
        'api_version': None,
        'serialization_format': None,
        'general_format': 'json',
    }
    assert REST_ACCEPT_RE.match('application/*').groupdict() == {
        'embedded_format': None,
        'api_version': None,
        'serialization_format': None,
        'general_format': '*',
    }
    assert REST_ACCEPT_RE.match('application/vnd.kafka+json').groupdict() == {
        'embedded_format': None,
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }

    # Embdded format
    assert REST_ACCEPT_RE.match('application/vnd.kafka.avro+json').groupdict() == {
        'embedded_format': 'avro',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_ACCEPT_RE.match('application/vnd.kafka.json+json').groupdict() == {
        'embedded_format': 'json',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_ACCEPT_RE.match('application/vnd.kafka.binary+json').groupdict() == {
        'embedded_format': 'binary',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_ACCEPT_RE.match('application/vnd.kafka.jsonschema+json').groupdict() == {
        'embedded_format': 'jsonschema',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }

    # API version
    assert REST_ACCEPT_RE.match('application/vnd.kafka.v1+json').groupdict() == {
        'embedded_format': None,
        'api_version': 'v1',
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_ACCEPT_RE.match('application/vnd.kafka.v2+json').groupdict() == {
        'embedded_format': None,
        'api_version': 'v2',
        'serialization_format': 'json',
        'general_format': None,
    }


def test_content_type_re():
    # incomplete headers
    assert not REST_CONTENT_TYPE_RE.match('')
    assert not REST_CONTENT_TYPE_RE.match('application/')
    assert not REST_CONTENT_TYPE_RE.match('application/vnd.kafka')
    assert not REST_CONTENT_TYPE_RE.match('application/vnd.kafka.json')
    # Unsupported serialization formats
    assert not REST_CONTENT_TYPE_RE.match('application/vnd.kafka+avro')
    assert not REST_CONTENT_TYPE_RE.match('application/vnd.kafka+protobuf')
    assert not REST_CONTENT_TYPE_RE.match('application/vnd.kafka+binary')
    # Unspecified format
    assert not REST_CONTENT_TYPE_RE.match('application/*')

    assert REST_CONTENT_TYPE_RE.match('application/json').groupdict() == {
        'embedded_format': None,
        'api_version': None,
        'serialization_format': None,
        'general_format': 'json',
    }
    assert REST_CONTENT_TYPE_RE.match('application/octet-stream').groupdict() == {
        'embedded_format': None,
        'api_version': None,
        'serialization_format': None,
        'general_format': 'octet-stream',
    }
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka+json').groupdict() == {
        'embedded_format': None,
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }

    # Embdded format
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka.avro+json').groupdict() == {
        'embedded_format': 'avro',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka.json+json').groupdict() == {
        'embedded_format': 'json',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka.binary+json').groupdict() == {
        'embedded_format': 'binary',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka.jsonschema+json').groupdict() == {
        'embedded_format': 'jsonschema',
        'api_version': None,
        'serialization_format': 'json',
        'general_format': None,
    }

    # API version
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka.v1+json').groupdict() == {
        'embedded_format': None,
        'api_version': 'v1',
        'serialization_format': 'json',
        'general_format': None,
    }
    assert REST_CONTENT_TYPE_RE.match('application/vnd.kafka.v2+json').groupdict() == {
        'embedded_format': None,
        'api_version': 'v2',
        'serialization_format': 'json',
        'general_format': None,
    }
