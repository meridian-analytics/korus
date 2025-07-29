import inquirer
from datetime import datetime


def parse_value(field, value):
    if field.type == datetime:
        return parse_datetime(None, current=value, required=field.required)

    elif field.type == int:
        return parse_int(None, current=value, required=field.required)

    elif field.type == float:
        return parse_float(None, current=value, required=field.required)

    else:
        return value


def parse_float(answers, current, required=False, return_bool=False):
    if required and current == "":
        raise inquirer.errors.ValidationError(
            "", reason="A non-null value is required for this field."
        )

    try:
        v = float(current) if current != "" else None
        return True if return_bool else v

    except:
        raise inquirer.errors.ValidationError(
            "", reason="Invalid input. Please enter a valid floating-point number."
        )


def validate_float(answers, current):
    return parse_float(answers, current, required=False, return_bool=True)


def validate_float_required(answers, current):
    return parse_float(answers, current, required=True, return_bool=True)


def parse_int(answers, current, required=False, return_bool=False):
    if required and current == "":
        raise inquirer.errors.ValidationError(
            "", reason="A non-null value is required for this field."
        )

    try:
        v = int(current) if current != "" else None
        return True if return_bool else v

    except:
        raise inquirer.errors.ValidationError(
            "", reason="Invalid input. Please enter a valid integer number."
        )


def validate_int(answers, current):
    return parse_int(answers, current, required=False, return_bool=True)


def validate_int_required(answers, current):
    return parse_int(answers, current, required=True, return_bool=True)


DATETIME_FORMAT = "YYYY-MM-DD HH:MM:SS.SSS"

DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H",
    "%Y-%m-%d",
    "%Y-%m",
    "%Y",
]


def parse_datetime(answers, current, required=False, return_bool=False):
    if current == "":
        if required:
            raise inquirer.errors.ValidationError(
                "", reason="A non-null value is required for this field."
            )

        else:
            return True if return_bool else None

    for fmt in DATETIME_FORMATS:
        try:
            v = datetime.strptime(current, fmt)
            return True if return_bool else v

        except:
            continue

    raise inquirer.errors.ValidationError(
        "", reason=f"Invalid input. Please enter a valid date-time as {DATETIME_FORMAT}"
    )


def validate_datetime(answers, current):
    return parse_datetime(answers, current, required=False, return_bool=True)


def validate_datetime_required(answers, current):
    return parse_datetime(answers, current, required=True, return_bool=True)
