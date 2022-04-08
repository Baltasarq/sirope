# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import uuid
from typing import Optional

from sirope.oid import OID


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

    def exists_for(self, oid: OID) -> Optional[str]:
        """Returns the safe oid for this OID, or None if does not exist."""
        soid = None
        bsoid = self._redis.hget(SafeIndex.OIDS_INDEXES_STORE_NAME,
                             str(oid))
        
        if bsoid:
            soid = bsoid.decode("utf-8", "replace")

        return soid

    def get_for(self, soid: str) -> Optional[OID]:
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
