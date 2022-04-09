import json


def serialize_dict(to_serialize: dict):
    return json.dumps(to_serialize).encode("utf-8")
