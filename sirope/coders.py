# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import json
import datetime
import base64

from sirope.oid import OID
from sirope.utils import full_name_from_obj


class Transcoder:
    CLASS_ID = "__class__"
    BYTES_ID = "__bytes__"


class JSONCoder(json.JSONEncoder):
    @staticmethod
    def build_time_dict(t: datetime.time) -> "dict[str, str|int]":
        return {Transcoder.CLASS_ID: full_name_from_obj(t),
                "ms": t.microsecond,
                "h": t.hour, "minute": t.minute, "s": t.second}

    @staticmethod
    def build_date_dict(d: datetime.date) -> "dict[str, str|int]":
        return {Transcoder.CLASS_ID: full_name_from_obj(d),
                "d": d.day, "month": d.month, "y": d.year}

    @staticmethod
    def build_bytes_dict(b: bytes) -> "dict[str, str]":
        return {Transcoder.CLASS_ID: Transcoder.BYTES_ID,
                "d": base64.b64encode(b).decode("ascii")}

    def default(self, obj):
        if isinstance(obj, OID):
            toret = obj.__dict__
            toret.update({Transcoder.CLASS_ID: full_name_from_obj(OID)})
            return toret
        elif isinstance(obj, datetime.datetime):
            toret = JSONCoder.build_date_dict(obj)
            toret.update(JSONCoder.build_time_dict(obj))
            toret[Transcoder.CLASS_ID] = full_name_from_obj(obj)
            return toret
        elif isinstance(obj, datetime.time):
            return JSONCoder.build_time_dict(obj)
        elif isinstance(obj, datetime.date):
            return JSONCoder.build_date_dict(obj)
        elif isinstance(obj, bytes):
            return JSONCoder.build_bytes_dict(obj)

        return json.JSONEncoder.default(self, obj)


class JSONDCoder(json.JSONDecoder):
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=JSONDCoder.from_dict)

    @staticmethod
    def date_from_dict(d: dict) -> datetime.date:
        return datetime.date(d["y"], d["month"], d["d"])

    @staticmethod
    def time_from_dict(d: dict) -> datetime.time:
        return datetime.time(d["h"], d["minute"], d["s"], d["ms"])

    @staticmethod
    def bytes_from_dict(d: dict) -> bytes:
        return base64.b64decode(d["d"].encode())

    @staticmethod
    def from_dict(d: dict) -> object:
        cls_name = d.get(Transcoder.CLASS_ID)

        if cls_name == full_name_from_obj(datetime.datetime):
            dt = JSONDCoder.date_from_dict(d)
            tm = JSONDCoder.time_from_dict(d)
            return datetime.datetime(dt.year, dt.month, dt.day,
                                     tm.hour, tm.minute, tm.second,
                                     tm.microsecond)
        elif cls_name == full_name_from_obj(OID):
            return OID.from_dict(d)
        elif cls_name == full_name_from_obj(datetime.date):
            return JSONDCoder.date_from_dict(d)
        elif cls_name == full_name_from_obj(datetime.time):
            return JSONDCoder.time_from_dict(d)
        elif cls_name == Transcoder.BYTES_ID:
            return JSONDCoder.bytes_from_dict(d)

        return d
