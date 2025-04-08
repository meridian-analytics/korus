import korus.database.interface as itf


def test_create_a_field():
    # without default value
    c = itf.Field("deployment_id", int, None, "Deployment identifier")
    assert c.name == "deployment_id"
    assert c.type == int
    assert c.default is None
    assert c.description == "Deployment identifier"

    # with default value
    c = itf.Field("channel", int, 0, "Audio channel")
    assert c.default == 0


def test_file_interface():
    f = itf.FileInterface()
    print(f)