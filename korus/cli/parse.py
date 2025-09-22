import inquirer
from datetime import datetime


def join(fcns: list):
    def validate(answers, current):
        res = True
        for fcn in fcns:
            res *= fcn(answers, current)

        return res

    return validate


def create_validate_index(table):
    def validate(answers, current):
        id = int(current)
        all_indices = table.reset_filter().filter().indices
        if id not in all_indices:
            raise inquirer.errors.ValidationError(
                "", reason="Invalid index. Please enter a valid index."
            )

        return True

    return validate


def create_validate_float(required=False):
    def validate(answers, current):
        return parse_float(answers, current, required=required, return_bool=True)

    return validate


def create_validate_int(required=False):
    def validate(answers, current):
        return parse_int(answers, current, required=required, return_bool=True)

    return validate


def create_validate_datetime(required=False):
    def validate(answers, current):
        return parse_datetime(answers, current, required=required, return_bool=True)

    return validate


def _parse_any(fcn, answers, current, required=False, return_bool=False):
    if required and current == "":
        raise inquirer.errors.ValidationError(
            "", reason="A non-null value is required for this field."
        )

    try:
        v = fcn(current)
        return True if return_bool else v

    except:
        raise inquirer.errors.ValidationError(
            "", reason="Invalid input. Please enter a valid floating-point number."
        )


def parse_value(value, field_type, required):
    if field_type == datetime:
        return parse_datetime(None, current=value, required=required)

    elif field_type == int:
        return parse_int(None, current=value, required=required)

    elif field_type == float:
        return parse_float(None, current=value, required=required)

    elif field_type == bool:
        return parse_bool(None, current=value, required=required)

    else:
        return value


def parse_bool(answers, current, required=False, return_bool=False):
    return _parse_any(bool, answers, current, required, return_bool)


def parse_float(answers, current, required=False, return_bool=False):
    return _parse_any(float, answers, current, required, return_bool)


def parse_int(answers, current, required=False, return_bool=False):
    return _parse_any(int, answers, current, required, return_bool)


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

    # try ISO first
    try:
        v = datetime.fromisoformat(current)
        return True if return_bool else v

    except:
        # then try a bunch of other formats
        for fmt in DATETIME_FORMATS:
            try:
                v = datetime.strptime(current, fmt)
                return True if return_bool else v

            except:
                continue

    raise inquirer.errors.ValidationError(
        "", reason=f"Invalid input. Please enter a valid date-time as {DATETIME_FORMAT}"
    )
