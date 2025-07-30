from termcolor import colored


def question(msg: str) -> str:
    """Format string as a PROMPT message"""
    return "[" + colored("?", "yellow") + "]" + msg + ": "


def info(msg: str) -> str:
    """Format string as an INFO message"""
    return colored(">> ", "green") + colored(msg, "white", attrs=["bold"]) + "\n"


def error(msg: str) -> str:
    """Format string as an ERROR message"""
    return colored(">> ", "red") + colored(msg, "white", attrs=["bold"]) + "\n"


def header(table_name: str = None, field_name: str = None) -> str:
    """Create header for console prompt"""
    h = ["main"]

    if table_name is not None:
        h.append(table_name)

    if field_name is not None:
        h.append(field_name)

    h[-1] = colored(h[-1], "white", attrs=["bold"])

    h = "|".join(h)
    if len(h) > 0:
        h = "[" + h + "] "

    return h
