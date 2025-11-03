from termcolor import colored


def bold(x: str, color: str = "white") -> str:
    return colored(x, color, attrs=["bold"])


def question(msg: str) -> str:
    """Format string as a PROMPT message"""
    return "[" + colored("?", "yellow") + "] " + msg + ": "


def info(msg: str, newline: bool = True) -> str:
    """Format string as an INFO message"""
    s = colored(">> ", "green") + colored(msg, "white", attrs=["bold"])
    if newline:
        s += "\n"

    return s


def warn(msg: str, newline: bool = True) -> str:
    """Format string as a WARNING message"""
    s = colored(">> ", "yellow") + colored(msg, "white", attrs=["bold"])
    if newline:
        s += "\n"

    return s


def error(msg: str, newline: bool = True) -> str:
    """Format string as an ERROR message"""
    s = colored(">> ", "red") + colored(msg, "white", attrs=["bold"])
    if newline:
        s += "\n"

    return s


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
