# Copyright 2026 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import unittest

from google.cloud.sqlalchemy_spanner.sqlalchemy_spanner import (
    SpannerDialect,
    _make_json_serializer,
)
from google.cloud.spanner_v1.data_types import JsonObject


def _custom_serializer(obj):
    """Sample json_serializer that handles datetime objects."""
    return json.dumps(obj, default=_datetime_default)


def _datetime_default(obj):
    """Sample default handler for json.dumps."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class TestMakeJsonSerializer(unittest.TestCase):
    """Tests for _make_json_serializer factory."""

    def test_json_object_subclass_returned_directly(self):
        result = _make_json_serializer(JsonObject)
        assert result is JsonObject

    def test_custom_subclass_returned_directly(self):
        class MyJsonObject(JsonObject):
            pass

        result = _make_json_serializer(MyJsonObject)
        assert result is MyJsonObject

    def test_callable_produces_json_object(self):
        factory = _make_json_serializer(_custom_serializer)
        obj = factory({"key": "value", "num": 42})
        assert isinstance(obj, JsonObject)
        parsed = json.loads(obj.serialize())
        assert parsed == {"key": "value", "num": 42}

    def test_callable_handles_datetime(self):
        factory = _make_json_serializer(_custom_serializer)
        dt = datetime.datetime(2023, 6, 15)
        obj = factory({"ts": dt})
        assert isinstance(obj, JsonObject)
        parsed = json.loads(obj.serialize())
        assert parsed["ts"] == "2023-06-15T00:00:00"

    def test_callable_handles_nested_datetimes(self):
        factory = _make_json_serializer(_custom_serializer)
        obj = factory({
            "events": [
                {"ts": datetime.datetime(2023, 1, 1), "action": "created"},
                {"ts": datetime.datetime(2023, 6, 15), "action": "updated"},
            ]
        })
        parsed = json.loads(obj.serialize())
        assert parsed["events"][0]["ts"] == "2023-01-01T00:00:00"
        assert parsed["events"][1]["ts"] == "2023-06-15T00:00:00"

    def test_callable_handles_arrays(self):
        factory = _make_json_serializer(_custom_serializer)
        obj = factory([1, 2, 3])
        assert isinstance(obj, JsonObject)
        assert json.loads(obj.serialize()) == [1, 2, 3]

    def test_callable_handles_null(self):
        factory = _make_json_serializer(lambda v: json.dumps(v))
        obj = factory(None)
        assert isinstance(obj, JsonObject)
        assert obj.serialize() is None

    def test_no_custom_types_remain_in_json_object(self):
        """After serialize-then-wrap, the JsonObject contains only native types."""
        factory = _make_json_serializer(_custom_serializer)
        dt = datetime.datetime(2023, 6, 15, 9, 30, 0)
        obj = factory({"ts": dt, "name": "test"})
        assert isinstance(obj["ts"], str)
        assert obj["ts"] == "2023-06-15T09:30:00"


class TestSpannerDialectJsonSerializer(unittest.TestCase):
    """Tests for json_serializer/json_deserializer support in SpannerDialect."""

    def test_default_json_serializer_is_json_object(self):
        dialect = SpannerDialect()
        assert dialect._json_serializer is JsonObject

    def test_default_json_deserializer_is_json_object(self):
        dialect = SpannerDialect()
        assert dialect._json_deserializer is JsonObject

    def test_custom_json_serializer_produces_factory(self):
        dialect = SpannerDialect(json_serializer=_custom_serializer)
        assert dialect._json_serializer is not JsonObject
        obj = dialect._json_serializer({"ts": datetime.datetime(2023, 1, 1)})
        assert isinstance(obj, JsonObject)
        parsed = json.loads(obj.serialize())
        assert parsed["ts"] == "2023-01-01T00:00:00"

    def test_json_object_subclass_used_directly(self):
        dialect = SpannerDialect(json_serializer=JsonObject)
        assert dialect._json_serializer is JsonObject

    def test_custom_json_deserializer(self):
        custom = lambda x: json.loads(x)
        dialect = SpannerDialect(json_deserializer=custom)
        assert dialect._json_deserializer is custom

    def test_class_attribute_unchanged_after_instance_override(self):
        _ = SpannerDialect(json_serializer=_custom_serializer)
        assert SpannerDialect._json_serializer is JsonObject

    def test_json_serializer_accepted_by_get_cls_kwargs(self):
        from sqlalchemy.util import get_cls_kwargs

        kwargs = get_cls_kwargs(SpannerDialect)
        assert "json_serializer" in kwargs
        assert "json_deserializer" in kwargs


class TestEndToEndJsonSerialization(unittest.TestCase):
    """End-to-end: SQLAlchemy JSON bind_processor -> serialize-then-wrap -> JsonObject.

    Simulates the full pipeline that occurs during a DML INSERT/UPDATE
    with a JSON column containing non-natively-serializable types.
    """

    def test_bind_processor_with_custom_serializer(self):
        """Simulate SQLAlchemy's JSON.bind_processor using the dialect."""
        from sqlalchemy import types as sa_types

        dialect = SpannerDialect(json_serializer=_custom_serializer)
        processor = sa_types.JSON().bind_processor(dialect)

        dt = datetime.datetime(2023, 6, 15, 9, 30, 0)
        value = {"event": "deploy", "timestamp": dt, "count": 42}

        result = processor(value)

        assert isinstance(result, JsonObject)
        serialized = result.serialize()
        parsed = json.loads(serialized)
        assert parsed["event"] == "deploy"
        assert parsed["timestamp"] == "2023-06-15T09:30:00"
        assert parsed["count"] == 42

    def test_bind_processor_with_nested_datetimes(self):
        from sqlalchemy import types as sa_types

        dialect = SpannerDialect(json_serializer=_custom_serializer)
        processor = sa_types.JSON().bind_processor(dialect)

        value = {
            "history": [
                {"ts": datetime.datetime(2023, 1, 1), "action": "created"},
                {"ts": datetime.datetime(2023, 6, 15), "action": "updated"},
            ]
        }
        result = processor(value)
        parsed = json.loads(result.serialize())
        assert parsed["history"][0]["ts"] == "2023-01-01T00:00:00"
        assert parsed["history"][1]["ts"] == "2023-06-15T00:00:00"

    def test_bind_processor_with_null_default(self):
        """With none_as_null=False (default), None becomes a null JsonObject."""
        from sqlalchemy import types as sa_types

        dialect = SpannerDialect(json_serializer=_custom_serializer)
        processor = sa_types.JSON().bind_processor(dialect)

        result = processor(None)
        assert isinstance(result, JsonObject)
        assert result.serialize() is None

    def test_bind_processor_with_null_as_sql_null(self):
        """With none_as_null=True, None becomes Python None (SQL NULL)."""
        from sqlalchemy import types as sa_types

        dialect = SpannerDialect(json_serializer=_custom_serializer)
        processor = sa_types.JSON(none_as_null=True).bind_processor(dialect)

        result = processor(None)
        assert result is None

    def test_spanner_helpers_pipeline(self):
        """Simulate _helpers._make_param_value_pb: isinstance check + bare serialize().

        _helpers.py checks isinstance(value, JsonObject) then calls
        value.serialize() with no arguments. Verify this works after
        the serialize-then-wrap round-trip.
        """
        dialect = SpannerDialect(json_serializer=_custom_serializer)
        factory = dialect._json_serializer

        dt = datetime.datetime(2023, 12, 25, 0, 0, 0)
        obj = factory({"holiday": "christmas", "date": dt})

        assert isinstance(obj, JsonObject)
        serialized = obj.serialize()
        assert serialized is not None
        parsed = json.loads(serialized)
        assert parsed["date"] == "2023-12-25T00:00:00"

    def test_default_dialect_unchanged(self):
        """Without json_serializer, the dialect uses plain JsonObject (no round-trip)."""
        from sqlalchemy import types as sa_types

        dialect = SpannerDialect()
        processor = sa_types.JSON().bind_processor(dialect)

        value = {"name": "test", "count": 42}
        result = processor(value)
        assert type(result) is JsonObject
        parsed = json.loads(result.serialize())
        assert parsed == {"count": 42, "name": "test"}


if __name__ == "__main__":
    unittest.main()
