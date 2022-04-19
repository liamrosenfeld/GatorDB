import json


def serialize_dict(to_serialize: dict) -> bytes:
    return json.dumps(to_serialize).encode("utf-8")


def bolden(msg: str) -> str:
    return "\033[1m" + msg + "\033[0m"


def print_bold(msg: str) -> None:
    print(bolden(msg))


def print_red(msg: str) -> None:
    print("\033[31m" + msg + "\033[0m")


def print_green(msg: str) -> None:
    print("\033[32m" + msg + "\033[0m")
