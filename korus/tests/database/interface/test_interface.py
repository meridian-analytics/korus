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


def test_add_get_set_data(in_memory_table_backend):
    """ Check that we can add data to, retrieve data from, and modify data in a TableInterface instance"""
    i = itf.interface.TableInterface("test_interface", in_memory_table_backend)

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

    t0 = datetime(2022, 12, 2)
    i.add_field("B", datetime, "another test field", default=t0)

    # no AssertionError raised when field as default value
    i.add(row)

    # retrieve all data
    rows = i.get()
    assert rows == [(2, None), (2, t0)]

    # retrieve only the 2nd entry
    rows = i.get(indices=1)
    assert rows == [(2, t0)]

    # retrieve only the 2nd field
    rows = i.get(fields="B")
    assert rows == [(None,), (t0,)]