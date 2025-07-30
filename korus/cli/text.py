from termcolor import colored


def info(msg):
    return colored(">> ", "green") + colored(msg, "white", attrs=["bold"]) + "\n"


def error(msg):
    return colored(">> ", "red") + colored(msg, "white", attrs=["bold"]) + "\n"


def header(table_name: str = None, field_name: str = None):
    h = ["main"]

    if table_name is not None:
        h.append(table_name)

    if field_name is not None:
        h.append(field_name)

    h = "|".join(h)
    if len(h) > 0:
        h = "[" + h + "] "

    return h
