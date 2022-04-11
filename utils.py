import json


def serialize_dict(to_serialize: dict):
    return json.dumps(to_serialize).encode("utf-8")


def serialize_str(to_serialize: str):
    return bytes(to_serialize, "utf-8")
