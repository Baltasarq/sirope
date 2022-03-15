# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


from collections import defaultdict
import datetime
import redis
import json
import sys


class OID:
    """Uniquely represents a given object in the store."""
    def __init__(self, cls: type, num: int):
        self._ns = Sirope._get_full_name(cls)
        self._num = int(num)

    @classmethod
    def from_dict(cls, d):
        toret = cls(None, 0)
        toret._ns = d["namespace"]
        toret._num = int(d["num"])
        return toret

    @property
    def num(self) -> int:
        return self._num

    @property
    def namespace(self) -> str:
        return self._ns

    def __hash__(self) -> int:
        return hash((self.namespace, self.num))

    def __eq__(self, other: object) -> bool:
        toret = False

        if isinstance(other, OID):
            toret = (self.namespace == other.namespace
                    and self.num == other.num)

        return toret

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __str__(self) -> str:
        return self._ns + "@" + str(self._num)


class Transcoder:
    CLASS_ID = "__class__"


class JSONCoder(json.JSONEncoder):
    @staticmethod
    def build_time_dict(t: datetime.time):
        return {Transcoder.CLASS_ID: Sirope._get_full_name(t),
                "ms": t.microsecond,
                "h": t.hour, "minute": t.minute, "s": t.second}

    @staticmethod
    def build_date_dict(d: datetime.date):
        return {Transcoder.CLASS_ID: Sirope._get_full_name(d),
                "d": d.day, "month": d.month, "y": d.year}

    def default(self, obj):
        if isinstance(obj, OID):
            return {Transcoder.CLASS_ID: Sirope._get_full_name(OID),
                    "namespace": obj.namespace,
                    "num": obj.num}
        elif isinstance(obj, datetime.datetime):
            toret = JSONCoder.build_date_dict(obj)
            toret.update(JSONCoder.build_time_dict(obj))
            toret[Transcoder.CLASS_ID] = Sirope._get_full_name(obj)
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

        if cls_name == Sirope._get_full_name(datetime.datetime):
            dt = JSONDCoder.date_from_dict(d)
            tm = JSONDCoder.time_from_dict(d)
            return datetime.datetime(dt.year, dt.month, dt.day,
                                     tm.hour, tm.minute, tm.second,
                                     tm.microsecond)
        elif cls_name == Sirope._get_full_name(OID):
            return OID.from_dict(d)
        elif cls_name == Sirope._get_full_name(datetime.date):
            return JSONDCoder.date_from_dict(d)
        elif cls_name == Sirope._get_full_name(datetime.time):
            return JSONDCoder.time_from_dict(d)

        return d


class Sirope:
    OID_ID = "__oid__"

    def __init__(self):
        self._redis = redis.Redis()

    def save(self, obj: object) -> OID:
        """Saves an object to the Redis store."""
        oid = obj.__dict__.get(Sirope.OID_ID)

        if not oid:
            ns = self._get_full_name(obj)
            oid = OID(obj.__class__, self._redis.hlen(ns))

            # Add the oid to the object
            obj.__dict__[Sirope.OID_ID] = oid
        
        self._redis.hset(
            oid.namespace,
            str(oid.num),
            Sirope._json_from_obj(obj))

        return obj.__dict__[Sirope.OID_ID]

    def load(self, oid: OID) -> object:
        """Loads an object from the Redis store"""
        ns = oid.namespace
        cls = Sirope._cls_from_str(ns)

        if not cls:
            raise NameError(ns)

        str_num = str(oid.num)
        raw_json = self._redis.hget(ns, str_num)
        txt_json = raw_json.decode("utf-8", "replace") if raw_json else ""
        return Sirope._obj_from_json(cls, txt_json)

    def exists(self, oid: OID) -> bool:
        """Determines whether an object exists or not."""
        return self._redis.hexists(oid.namespace, str(oid.num))

    def delete(self, oid: OID) -> bool:
        """Deletes a given object."""
        return self._redis.hdel(oid.namespace, str(oid.num)) > 0

    def multi_delete(self, oids: list[OID]) -> None:
        """Deletes multiple objects"""
        if oids:
            ns = oids[0]
            dict_objs = defaultdict(list)

            for oid in oids:
                dict_objs[oid.namespace].append(str(oid.num))

            for ns, lnums in dict_objs.items():
                self._redis.hdel(ns, *lnums)

    def num_objs_for(self, cls: type):
        """Returns the number of objects stored for this class."""
        return self._redis.hlen(Sirope._get_full_name(cls))

    def load_all_of(self, cls: type) -> list[object]:
        """Returns all objects stored for this class."""
        json_objs = self._redis.hvals(Sirope._get_full_name(cls))
        
        return [Sirope._obj_from_json(cls, value.decode("utf-8", "replace"))
                for value in json_objs]

    def load_multi_of(self, cls: type, oids: list[OID]) -> list[object]:
        """Loads the objects corresponding to the oids for this class."""
        keys = [str(oid.num) for oid in oids]
        json_objects = self._redis.hmget(Sirope._get_full_name(cls), *keys)

        return [Sirope._obj_from_json(cls, jobj.decode("utf-8", "replace")) for jobj in json_objects]

    def load_all_keys_of(self, cls: type) -> list[OID]:
        """Returns a list of oid's of stored objects for this class."""
        ns = Sirope._get_full_name(cls)
        keys = self._redis.hkeys(ns)
        return [OID.from_dict({
                            "namespace": ns,
                            "num": k.decode("utf-8", "?")})
                    for k in keys]

    @staticmethod
    def _cls_from_str(path: str) -> type:
        path_parts = path.split('.')
        cls_name = path_parts.pop()
        mdl_name = ".".join(path_parts)
        mdl = sys.modules.get(mdl_name)
        return mdl.__dict__.get(cls_name) if mdl else None

    @staticmethod
    def _get_full_name(o: object):
        """Returns the full qualified name of the class of this object."""
        cl = o.__class__ if not isinstance(o, type) else o
        module = cl.__module__
        cls_name = cl.__name__

        if module is None or module == str.__class__.__module__:
            return cls_name

        return module + '.' + cls_name

    @staticmethod
    def _obj_from_json(cls: object, json_txt: str) -> object:
        toret = cls()
        obj_dict = JSONDCoder().decode(json_txt)
        obj_dict.pop("__class__", None)
        toret.__dict__ = obj_dict
        return toret

    @staticmethod
    def _json_from_obj(obj: object) -> str:
        return JSONCoder().encode(obj.__dict__)
