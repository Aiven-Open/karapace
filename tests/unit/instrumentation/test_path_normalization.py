"""
Tests for prometheus path normalization.

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations


from karapace.core.instrumentation.prometheus import normalize_path


class TestNormalizePath:
    """Tests for the normalize_path function."""

    def test_health_endpoint_unchanged(self) -> None:
        """Health endpoint should not be modified."""
        assert normalize_path("/_health") == "/_health"

    def test_metrics_endpoint_unchanged(self) -> None:
        """Metrics endpoint should not be modified."""
        assert normalize_path("/metrics") == "/metrics"

    def test_topic_name_normalized(self) -> None:
        """Topic names should be replaced with {topic}."""
        assert normalize_path("/topics/my-topic") == "/topics/{topic}"
        assert normalize_path("/topics/yunextraffic.fct.yuhubiot_live_raw.1") == "/topics/{topic}"

    def test_consumer_group_uuid_normalized(self) -> None:
        """Consumer group UUIDs should be replaced with {uuid}."""
        path = "/consumers/3f410583-cfa3-48e8-bd9f-22eff1881c00"
        assert normalize_path(path) == "/consumers/{uuid}"

    def test_consumer_instance_uuid_normalized(self) -> None:
        """Consumer instance UUIDs should be replaced with {uuid}."""
        path = "/consumers/3f410583-cfa3-48e8-bd9f-22eff1881c00/instances/0333f5a1-d242-42bc-ba35-d7c5d67fc121"
        assert normalize_path(path) == "/consumers/{uuid}/instances/{uuid}"

    def test_consumer_records_path_normalized(self) -> None:
        """Full consumer records path should be normalized."""
        path = "/consumers/3f410583-cfa3-48e8-bd9f-22eff1881c00/instances/0333f5a1-d242-42bc-ba35-d7c5d67fc121/records"
        assert normalize_path(path) == "/consumers/{uuid}/instances/{uuid}/records"

    def test_consumer_subscription_path_normalized(self) -> None:
        """Consumer subscription path should be normalized."""
        path = "/consumers/3f410583-cfa3-48e8-bd9f-22eff1881c00/instances/0333f5a1-d242-42bc-ba35-d7c5d67fc121/subscription"
        assert normalize_path(path) == "/consumers/{uuid}/instances/{uuid}/subscription"

    def test_schema_id_normalized(self) -> None:
        """Schema IDs should be replaced with {id}."""
        assert normalize_path("/schemas/ids/42") == "/schemas/ids/{id}"
        assert normalize_path("/schemas/ids/12345") == "/schemas/ids/{id}"

    def test_subject_name_normalized(self) -> None:
        """Subject names should be replaced with {subject}."""
        assert normalize_path("/subjects/my-subject") == "/subjects/{subject}"
        assert normalize_path("/subjects/my-subject/versions") == "/subjects/{subject}/versions"

    def test_subject_version_normalized(self) -> None:
        """Subject versions should be replaced with {version}."""
        assert normalize_path("/subjects/my-subject/versions/3") == "/subjects/{subject}/versions/{version}"
        assert normalize_path("/subjects/my-subject/versions/latest") == "/subjects/{subject}/versions/latest"

    def test_multiple_normalizations(self) -> None:
        """Multiple dynamic segments should all be normalized."""
        # This tests that all patterns work together
        path = "/subjects/test-subject/versions/5"
        assert normalize_path(path) == "/subjects/{subject}/versions/{version}"

    def test_uppercase_uuid_normalized(self) -> None:
        """Uppercase UUIDs should also be normalized."""
        path = "/consumers/3F410583-CFA3-48E8-BD9F-22EFF1881C00"
        assert normalize_path(path) == "/consumers/{uuid}"

    def test_mixed_case_uuid_normalized(self) -> None:
        """Mixed case UUIDs should also be normalized."""
        path = "/consumers/3f410583-CFA3-48e8-BD9F-22eff1881c00"
        assert normalize_path(path) == "/consumers/{uuid}"
