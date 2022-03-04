# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


from collections import defaultdict
import redis
import json


class OID:
    """Uniquely represents a given object in the store."""
    def __init__(self, ns: str, num: int):
        if isinstance(ns, type):
            self._ns = ns.__name__
        else:
            self._ns = ns.strip()

        self._num = int(num)

    @property
    def num(self) -> int:
        return self._num

    @property
    def namespace(self) -> str:
        return self._ns

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


class Sirope:
    def __init__(self):
        self._redis = redis.Redis()

    def save(self, obj: object) -> OID:
        """Saves an object to the Redis store."""
        ns = obj.__class__.__name__
        num_objs = self._redis.hlen(ns)

        # Add the oid to the object, only for info purposes
        obj.oid = OID(ns, num_objs)
        
        self._redis.set(
            ns,
            str(num_objs),
            Sirope._json_from_obj(obj))

        return obj.oid

    def load(self, oid: OID, obj: object, verify=True) -> object:
        """Loads an object from the Redis store"""
        ns = obj.__class__.__name__

        if (verify
        and ns != oid.namespace):
            raise NameError(ns + "??")

        str_num = str(oid.num)
        return Sirope._obj_from_json(
                    json.loads(self._redis.hget(ns, str_num)))

    def exists(self, oid: OID) -> bool:
        """Determines whether an object exists or not."""
        return self._redis.hexists(oid.ns, str(oid.num))

    def delete(self, oid: OID) -> bool:
        """Deletes a given object."""
        return self._redis.hdel(oid.ns, str(oid.num)) > 0

    def multi_delete(self, oids: list[OID]) -> None:
        """Deletes multiple objects"""
        if oids:
            ns = oid[0]
            dict_objs = defaultdict(list)

            for oid in oids:
                dict_objs[oid.ns].append(str(oid.num))

            for ns, lnums in dict_objs:
                self._redis.hdel(ns, *lnums)

    def num_objs_for(self, cls: object):
        """Returns the number of objects stored for this class."""
        return self._redis.hlen(cls.__name__)

    def load_all_from(self, cls: object) -> list[object]:
        """Returns all objects stored for this class."""
        json_objs = self._redis.hvals(cls.__name__)
        
        return [Sirope._obj_from_json(cls, value) for value in json_objs]

    def load_from(self, cls: object, oids: list[OID]) -> list[object]:
        """Loads the objects corresponding to the oids for this class."""
        keys = [str(oid.num) for oid in oids]
        json_objects = self._redis.hmget(cls.__name__, *keys)

        return [Sirope._obj_from_json(jobj) for jobj in json_objects]

    def load_all_keys_from(self, cls:object) -> list[OID]:
        """Returns a list of oid's of stored objects for this class."""
        ns = cls.__name__
        keys = self._redis.hkeys(ns)
        return [OID(ns, k.decode("utf-8", "?")) for k in keys]

    @staticmethod
    def _obj_from_json(cls: object, json_txt: str) -> object:
        toret = cls()
        toret.__dict__ = json.loads(json_txt)
        return toret

    @staticmethod
    def _json_from_obj(obj: object) -> str:
        return json.dumps(obj.__dict__)
