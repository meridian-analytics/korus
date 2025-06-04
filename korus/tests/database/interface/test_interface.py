import pytest
from datetime import datetime
import korus.database.interface as itf


def test_create_field_alias():
    a = itf.FieldAlias("label_id", "label", tuple, "a description")
    assert a.field_name == "label_id"
    assert a.name == "label"
    assert a.type == tuple
    assert a.description == "a description"


def test_create_a_field_definition():
    """Check that FieldDefinition class behaves as it should"""
    # without default value
    f = itf.FieldDefinition("deployment_id", int, "Deployment identifier", False, None)
    assert f.name == "deployment_id"
    assert f.type == int
    assert f.description == "Deployment identifier"
    assert f.required == False
    assert f.default is None

    # with default value
    f = itf.FieldDefinition("channel", int, "Audio channel", False, 0)
    assert f.default == 0


def test_add_get_set_data(in_memory_table_backend):
    """Check that we can add data to, retrieve data from, and modify data in a TableInterface instance"""
    i = itf.interface.TableInterface("test", in_memory_table_backend)

    i.add_field("A", int, "a test field", default=None)

    # AssertionError is raised when required field is missing and has null default value
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

    # no AssertionError raised when field has a non-null default value
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

    # modify the data
    t1 = datetime(2025, 1, 24)
    i.set(0, {"B": t1})
    i.set(1, {"A": 3})

    # retrieve all data and check that values have been updated
    rows = i.get()
    assert rows == [(2, t1), (3, t0)]

    # test that we can iterate over the rows
    rows = [row for row in iter(i)]
    assert rows == [(2, t1), (3, t0)]

    # check that we can create a string summary
    str(i)

    # create a row with restricted set of allowed values
    i.add_field("C", int, "yet another test field", options=[1, 3])

    # attempt to add illegal value fails
    row["C"] = 4
    with pytest.raises(AssertionError):
        i.add(row)

    # legal value is accepted
    row["C"] = 3
    i.add(row)


def test_use_alias(in_memory_table_backend):
    """Check that aliases work as they should"""
    i = itf.interface.TableInterface("test", in_memory_table_backend)

    i.add_field("A", int, "a test field", default=None)

    i.add_alias(
        "A",
        "AA",
        str,
        "a test alias",
        transform=lambda x, **_: int(x),
        reverse_transform=lambda x, **_: str(x),
    )

    # add using field name
    i.add({"A": 12})

    # add using alias
    i.add({"AA": "12"})

    # retrieve alias value
    values = i.get(fields="A")
    assert values == [("12",), ("12",)]

    # retrieve field value
    values = i.get(fields="A", alias=False)
    assert values == [(12,), (12,)]


def test_filter_data(in_memory_table_backend):
    """Check that we can filter data in a TableInterface instance"""
    i = itf.interface.TableInterface("test", in_memory_table_backend)

    i.add_field("A", int, "a test field", default=None)
    i.add_field("B", str, "another test field", default=None)

    i.add({"A": 11, "B": "x"})
    i.add({"A": 12, "B": "xy"})
    i.add({"A": 13, "B": "xyz"})

    # filter with single condition
    idx = i.filter({"A": 11}).indices
    assert idx == [0]

    # filter with two conditions
    i.reset_filter()
    idx = i.filter({"A": (11, 12), "B": "xy"}).indices
    assert idx == [1]

    # chained filter
    i.reset_filter()
    idx = i.filter({"A": (11, 12)}).filter({"B": "xy"}).indices
    assert idx == [1]


def test_get_as_pandas(in_memory_table_backend):
    """Check that we can get data as a Pandas DataFrame"""
    i = itf.interface.TableInterface("test", in_memory_table_backend)

    i.add_field("A", int, "a test field")
    i.add_field("B", datetime, "anoter test field")
    i.add_field("C", list, "yet another test field")
    i.add_field("D", dict, "the last test field")

    row = {
        "A": 11,
        "B": datetime(2022, 12, 2),
        "C": ["x", "y"],
        "D": {"alpha": 1, "beta": 2},
    }
    i.add(row)

    row = {
        "A": 12,
        "B": datetime(2022, 12, 3),
        "C": ["xx", "yy", "zz"],
        "D": {"gamma": 3},
    }
    i.add(row)

    df = i.get(as_pandas=True, return_indices=True)

    expected = """        A          B             C                        D
index                                                      
0      11 2022-12-02        [x, y]  {'alpha': 1, 'beta': 2}
1      12 2022-12-03  [xx, yy, zz]             {'gamma': 3}"""
    assert df.to_string() == expected
