import korus.database.interface as itf


def test_create_a_field_definition():
    # without default value
    c = itf.FieldDefinition("deployment_id", int, "Deployment identifier", None)
    assert c.name == "deployment_id"
    assert c.type == int
    assert c.default is None
    assert c.description == "Deployment identifier"

    # with default value
    c = itf.FieldDefinition("channel", int, "Audio channel", 0)
    assert c.default == 0


