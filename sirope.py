# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


from collections import defaultdict
import datetime
import redis
import json
import sys
import uuid


class OID:
    """Uniquely represents a given object in the store."""
    def __init__(self, cls: type, num: int):
        self._ns = Sirope._get_full_name(cls)
        self._num = int(num)

    @classmethod
    def from_pair(cls, p):
        """
            Create an OID from a pair.
            :param cls: OID class
            :param d: A pair with values (<namespace>, <num>)
            :return: The corresponding OID object.
        """
        toret = cls(None, 0)
        toret._ns = p[0]
        toret._num = int(p[1])
        return toret

    @classmethod
    def from_dict(cls, d):
        """
            Create an OID from a dict.
            :param cls: OID class
            :param d: A dict with the members 'namespace' and 'num'
            :return: The corresponding OID object.
        """
        return OID.from_pair((d["_ns"], int(d["_num"])))

    @classmethod
    def from_text(cls, toid: str):
        """Toma una cadena del tipo ns@1 y devuelve el OID."""
        oid_parts = toid.strip().split('@')
        return OID.from_pair((oid_parts[0], oid_parts[1]))

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
            toret = obj.__dict__
            toret.update({Transcoder.CLASS_ID: Sirope._get_full_name(OID)})
            return toret
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


class SafeIndex:
    INDEXES_OIDS_STORE_NAME = "__safe_indexes_oids__"
    OIDS_INDEXES_STORE_NAME = "__safe_oids_indexes"
    instance = None

    def __init__(self, redis):
        self._redis = redis

    def build_for(self, oid: OID) -> str:
        """Creates (if needed), a new safe id for this OID."""
        bsoid = self._redis.hget(SafeIndex.OIDS_INDEXES_STORE_NAME,
                             str(oid))
        
        if bsoid:
            soid = bsoid.decode("utf-8", "replace")
        else:
            soid = SafeIndex._create_soid()
            self._redis.hset(SafeIndex.INDEXES_OIDS_STORE_NAME,
                            soid, str(oid))
            self._redis.hset(SafeIndex.OIDS_INDEXES_STORE_NAME,
                            str(oid), soid)

        return soid

    def exists_for(self, oid: OID) -> str:
        """Returns the safe oid for this OID, or None if does not exist."""
        soid = None
        bsoid = self._redis.hget(SafeIndex.OIDS_INDEXES_STORE_NAME,
                             str(oid))
        
        if bsoid:
            soid = bsoid.decode("utf-8", "replace")

        return soid

    def get_for(self, soid: str) -> OID:
        """Returns the OID associated to this safe oid."""
        toret = None
        btoid = self._redis.hget(SafeIndex.INDEXES_OIDS_STORE_NAME,
                            soid)

        if btoid:
            toid = btoid.decode("utf-8", "replace")
            toret = OID.from_text(toid)

        return toret

    def delete_for(self, oid: OID):
        """Deletes the safe oid associated to this OID."""
        bsoid = self._redis.hget(SafeIndex.OIDS_INDEXES_STORE_NAME,
                                str(oid))

        if bsoid:
            soid = bsoid.decode("utf-8", "replace")
            self._redis.hdel(SafeIndex.INDEXES_OIDS_STORE_NAME,
                             soid)
            self._redis.hdel(SafeIndex.OIDS_INDEXES_STORE_NAME,
                             str(oid))

    def __len__(self):
        return self._redis.hlen(SafeIndex.INDEXES_OIDS_STORE_NAME)

    @staticmethod
    def _create_soid():
        return uuid.uuid4().hex

    @staticmethod
    def get(redis):
        if not SafeIndex.instance:
            SafeIndex.instance = SafeIndex(redis)

        return SafeIndex.instance


class Sirope:
    OID_ID = "__oid__"

    def __init__(self):
        self._redis = redis.Redis()
        self._indexes = SafeIndex.get(self._redis)

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
        self._indexes.delete_for(oid)
        return self._redis.hdel(oid.namespace, str(oid.num)) > 0

    def multi_delete(self, oids: list[OID]) -> None:
        """Deletes multiple objects"""
        if oids:
            dict_objs = defaultdict(list)

            for oid in oids:
                self._indexes.delete_for(oid)
                dict_objs[oid.namespace].append(str(oid.num))

            for ns, lnums in dict_objs.items():
                self._redis.hdel(ns, *lnums)

    def num_objs_for(self, cls: type):
        """Returns the number of objects stored for this class."""
        return self._redis.hlen(Sirope._get_full_name(cls))

    def num_of_safe_indexes(self) -> int:
        return len(self._indexes)

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
        return [OID.from_pair((ns, k.decode("utf-8", "replace")))
                for k in keys]

    def get_oid_from_safe(self, soid: str) -> OID:
        return self._indexes.get_for(soid)

    def build_safe_for_oid(self, oid: OID) -> str:
        return self._indexes.build_for(oid)

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
