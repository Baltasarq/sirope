# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


from collections import defaultdict
from typing import Callable
from typing import Iterable
import redis

from sirope.oid import OID
from sirope.coders import JSONCoder
from sirope.coders import JSONDCoder
from sirope.safeindex import SafeIndex
from sirope.utils import full_name_from_obj
from sirope.utils import cls_from_str


class Sirope:
    OID_ID = "__oid__"
    NEXT_IDS_ID = "__next_ids__"

    def __init__(self, redis_obj: redis.Redis=None):
        """Creates a Sirope object from a given Redis.
            :param redis: A Redis object or None.
        """
        if not redis_obj:
            self._redis = redis.Redis()
        else:
            self._redis = redis_obj
        self._indexes = SafeIndex.get(self._redis)

    def __create_next_id(self, ns: str):
        return self._redis.hincrby(Sirope.NEXT_IDS_ID, ns, 1) - 1

    def __get_next_id(self, ns: str):
        bytes_toret = self._redis.hget(Sirope.NEXT_IDS_ID, ns)

        if not bytes_toret:
            bytes_toret = b'0'

        return int(bytes_toret.decode("utf-8", "replace"))

    def save(self, obj: object) -> OID:
        """Saves an object to the Redis store."""
        oid = obj.__dict__.get(Sirope.OID_ID)

        if not oid:
            num_id = self.__create_next_id(full_name_from_obj(obj))
            oid = OID(obj.__class__, num_id)

            # Add the oid to the object
            obj.__dict__[Sirope.OID_ID] = oid

        self._redis.hset(
            oid.namespace,
            str(oid.num),
            Sirope.__json_from_obj(obj))

        return obj.__dict__[Sirope.OID_ID]

    def load(self, oid: OID) -> object:
        """Loads an object from the Redis store"""
        ns = oid.namespace
        cls = cls_from_str(ns)

        if not cls:
            raise NameError(ns)

        str_num = str(oid.num)
        return Sirope.__obj_from_json(cls, self._redis.hget(ns, str_num))

    def exists(self, oid: OID) -> bool:
        """Determines whether an object exists or not."""
        return self._redis.hexists(oid.namespace, str(oid.num))

    def delete(self, oid: OID) -> bool:
        """Deletes a given object."""
        self._indexes.delete_for(oid)
        return self._redis.hdel(oid.namespace, str(oid.num)) > 0

    def multi_delete(self, oids: "list[OID]") -> None:
        """Deletes multiple objects"""
        if oids:
            dict_objs = defaultdict(list)

            for oid in oids:
                self._indexes.delete_for(oid)
                dict_objs[oid.namespace].append(str(oid.num))

            for ns, lnums in dict_objs.items():
                self._redis.hdel(ns, *lnums)

    def num_objs(self, cls: type) -> int:
        """Returns the total number of objects stored for this class."""
        return self._redis.hlen(full_name_from_obj(cls))

    def num_safe_indexes(self) -> int:
        """Returns the total number of safe indexes for this class."""
        return len(self._indexes)

    def enumerate(self, cls: type, max: int = 0) -> Iterable[object]:
        """Returns all objects stored for this class, as an iterator."""
        num = 0
        for vp in self._redis.hscan_iter(full_name_from_obj(cls)):
            yield Sirope.__obj_from_json(cls, vp[1])

            num += 1
            if (max > 0
            and num >= max):
                break

    def load_all(self, cls: type) -> Iterable[object]:
        """Returns an iterable for all objects stored for this class."""
        json_objs = self._redis.hvals(full_name_from_obj(cls))
        for obj in json_objs:
            yield Sirope.__obj_from_json(cls, obj)

    def load_first(self, cls: type, num: int) -> Iterable[object]:
        """Returns the first max objects in stored order for this class."""
        # Determine num
        num = max(1, num)
        num_objs = self.num_objs(cls)
        num = min(num, num_objs)

        # Retrieve in store order
        i = 0
        cls_name = full_name_from_obj(cls)
        while num > 0:
            obj = self.load(OID.from_pair((cls_name, i)))
            i += 1

            if obj:
                num -= 1
                yield obj

    def load_last(self, cls: type, num: int) -> Iterable[object]:
        """Returns the last num objects in stored order for this class."""
        # Determine max
        num = max(num, 1)
        num_objs = self.num_objs(cls)
        num = min(num, num_objs)

        # Retrieve in store order
        cls_name = full_name_from_obj(cls)
        i = self.__get_next_id(cls_name) - 1
        while num > 0:
            obj = self.load(OID.from_pair((cls_name, i)))
            i -= 1

            if obj:
                num -= 1
                yield obj

    def multi_load(self, oids: "list[OID]") -> Iterable[object]:
        """Returns an iterable for the objects corresponding
           to the oids in the given list.
        """
        dict_objs = defaultdict(list)

        for oid in oids:
            dict_objs[oid.namespace].append(str(oid.num))

        for ns, keys in dict_objs.items():
            cls = cls_from_str(ns)
            for jobj in self._redis.hmget(ns, *keys):
                yield Sirope.__obj_from_json(cls, jobj)

    def load_all_keys(self, cls: type) -> Iterable[OID]:
        """Returns an iterable of oid's of stored objects for this class."""
        ns = full_name_from_obj(cls)
        keys = self._redis.hkeys(ns)
        for k in keys:
            yield OID.from_pair((ns, k))

    def filter(self, cls: type, pred: Callable, max: int=0) -> Iterable[object]:
        """Returns an iterable for the objects complaint with the pred."""
        ns = full_name_from_obj(cls)

        num = 0
        for vp in self._redis.hscan_iter(ns):
            obj = Sirope.__obj_from_json(cls, vp[1])

            if pred(obj):
                yield obj
                num += 1

            if max > 0 and num >= max:
                break

    def find_first(self, cls: type, pred: Callable) -> "object|None":
        """Returns the first object compliant with pred, or None."""
        ns = full_name_from_obj(cls)
        toret = None

        for vp in self._redis.hscan_iter(ns):
            obj = Sirope.__obj_from_json(cls, vp[1])

            if pred(obj):
                toret = obj
                break

        return toret

    def oid_from_safe(self, safe_oid: str) -> OID:
        return self._indexes.get_for(safe_oid)

    def safe_from_oid(self, oid: OID) -> str:
        return self._indexes.build_for(oid)

    @staticmethod
    def __obj_from_json(cls: type, json_txt: "str|bytes|None") -> object:
        if not cls:
            raise ValueError("invalid class")

        if not json_txt:
            raise ValueError("invalid json source")

        toret: object = object.__new__(cls)

        if isinstance(json_txt, bytes):
            json_txt = json_txt.decode("utf-8", "replace")

        obj_dict = JSONDCoder().decode(json_txt)
        obj_dict.pop("__class__", None)
        toret.__dict__ = obj_dict
        return toret

    @staticmethod
    def __json_from_obj(obj: object) -> str:
        return JSONCoder().encode(obj.__dict__)
