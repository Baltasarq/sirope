# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


from collections import defaultdict
from typing import Callable
import redis

from sirope.oid import OID
from sirope.coders import JSONCoder
from sirope.coders import JSONDCoder
from sirope.safeindex import SafeIndex
from sirope.utils import full_name_from_obj
from sirope.utils import cls_from_str


class Sirope:
    OID_ID = "__oid__"

    def __init__(self, redis_obj: redis.Redis=None):
        """Creates a Sirope object from a given Redis.
            :param redis: A Redis object or None.
        """
        if not redis_obj:
            self._redis = redis.Redis()
        else:
            self._redis = redis_obj
        self._indexes = SafeIndex.get(self._redis)

    def save(self, obj: object) -> OID:
        """Saves an object to the Redis store."""
        oid = obj.__dict__.get(Sirope.OID_ID)

        if not oid:
            ns = full_name_from_obj(obj)
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
        cls = cls_from_str(ns)

        if not cls:
            raise NameError(ns)

        str_num = str(oid.num)
        return Sirope._obj_from_json(cls, self._redis.hget(ns, str_num))

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

    def num_objs(self, cls: type):
        """Returns the number of objects stored for this class."""
        return self._redis.hlen(full_name_from_obj(cls))

    def num_safe_indexes(self) -> int:
        return len(self._indexes)

    def enumerate(self, cls: type):
        """Returns all objects stored for this class, as an iterator."""
        for vp in self._redis.hscan_iter(full_name_from_obj(cls)):
            yield Sirope._obj_from_json(cls, vp[1])

    def load_all(self, cls: type) -> list[object]:
        """Returns all objects stored for this class."""
        json_objs = self._redis.hvals(full_name_from_obj(cls))        
        return [Sirope._obj_from_json(cls, value) for value in json_objs]

    def load_multi(self, cls: type, oids: list[OID]) -> list[object]:
        """Loads the objects corresponding to the oids for this class."""
        keys = [str(oid.num) for oid in oids]
        json_objects = self._redis.hmget(full_name_from_obj(cls), *keys)

        return [Sirope._obj_from_json(cls, jobj) for jobj in json_objects]

    def load_all_keys(self, cls: type) -> list[OID]:
        """Returns a list of oid's of stored objects for this class."""
        ns = full_name_from_obj(cls)
        keys = self._redis.hkeys(ns)
        return [OID.from_pair((ns, k)) for k in keys]

    def filter(self, cls: type, pred: Callable) -> list[object]:
        ns = full_name_from_obj(cls)

        for vp in self._redis.hscan_iter(ns):
            obj = Sirope._obj_from_json(cls, vp[1])

            if pred(obj):
                yield obj

    def find_first(self, cls: type, pred: Callable) -> list[object]:
        ns = full_name_from_obj(cls)
        toret = None

        for vp in self._redis.hscan_iter(ns):
            obj = Sirope._obj_from_json(cls, vp[1])

            if pred(obj):
                toret = obj
                break

        return toret

    def get_oid_from_safe(self, soid: str) -> OID:
        return self._indexes.get_for(soid)

    def build_safe_for_oid(self, oid: OID) -> str:
        return self._indexes.build_for(oid)

    @staticmethod
    def _obj_from_json(cls: object, json_txt: str|bytes) -> object:
        toret = None

        if cls and json_txt:
            toret = cls()

            if isinstance(json_txt, bytes):
                json_txt = json_txt.decode("utf-8", "replace")

            obj_dict = JSONDCoder().decode(json_txt)
            obj_dict.pop("__class__", None)
            toret.__dict__ = obj_dict
        return toret

    @staticmethod
    def _json_from_obj(obj: object) -> str:
        return JSONCoder().encode(obj.__dict__)
