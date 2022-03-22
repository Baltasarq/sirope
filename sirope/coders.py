# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import json
import datetime

from sirope.oid import OID
from sirope.utils import full_name_from_obj


class Transcoder:
    CLASS_ID = "__class__"


class JSONCoder(json.JSONEncoder):
    @staticmethod
    def build_time_dict(t: datetime.time):
        return {Transcoder.CLASS_ID: full_name_from_obj(t),
                "ms": t.microsecond,
                "h": t.hour, "minute": t.minute, "s": t.second}

    @staticmethod
    def build_date_dict(d: datetime.date):
        return {Transcoder.CLASS_ID: full_name_from_obj(d),
                "d": d.day, "month": d.month, "y": d.year}

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

        return json.JSONEncoder.default(self, obj)


class JSONDCoder(json.JSONDecoder):
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=JSONDCoder.from_dict)

    @staticmethod
    def date_from_dict(d):
        return datetime.date(d["y"], d["month"], d["d"])

    @staticmethod
    def time_from_dict(d):
        return datetime.time(d["h"], d["minute"], d["s"], d["ms"])

    @staticmethod
    def from_dict(d):
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

        return d
