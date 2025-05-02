import pytest
from datetime import datetime
import korus.database.interface as itf


def test_create_a_field_definition():
    """ Check that FieldDefinition class behaves as it should"""
    # without default value
    c = itf.FieldDefinition("deployment_id", int, "Deployment identifier", None)
    assert c.name == "deployment_id"
    assert c.type == int
    assert c.default is None
    assert c.description == "Deployment identifier"

    # with default value
    c = itf.FieldDefinition("channel", int, "Audio channel", 0)
    assert c.default == 0


def test_add_data(dummy_backend):
    """ Check that we can add data to a TableInterface instance"""
    i = itf.interface.TableInterface("test_interface", dummy_backend)

    i.add_field("A", int, "a test field", default=None)

    # AssertionError is raised when required field is missing
    row = dict()
    with pytest.raises(AssertionError):
        i.add(row)

    # AssertionError is raised when field has wrong type
    row["A"] = 2.3
    with pytest.raises(AssertionError):
        i.add(row)

    # adding a value with the correct type works
    row["A"] = 2
    i.add(row)

    i.add_field("B", datetime, "another test field", default=datetime(2022, 12, 2))

    # no AssertionError raised when field as default value
    i.add(row)