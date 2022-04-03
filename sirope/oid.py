# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


from sirope.utils import full_name_from_obj


class OID:
    """Uniquely represents a given object in the store."""
    def __init__(self, cls: type, num: int):
        self._ns = full_name_from_obj(cls)
        self._num = int(num)

    @classmethod
    def from_pair(cls, p):
        """
            Create an OID from a pair.
            :param cls: OID class
            :param p: A pair with values (<namespace>, <num>)
            :return: The corresponding OID object.
        """
        toret = None

        if cls and p:
            toret = cls(None, 0)
            toret._ns = p[0]
            txt_num = p[1]

            if isinstance(txt_num, bytes):
                num = txt_num.decode("utf-8", "replace")

            toret._num = int(txt_num)

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
