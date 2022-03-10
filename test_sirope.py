# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import unittest
import sirope
import datetime


class Person:
    def __init__(self,
                 name: str="", born: datetime.datetime=None, email: str="",
                 creation_date: datetime.date=None,
                 creation_time: datetime.time=None):
        self._name = name
        self._born = born
        self._email = email
        self._creation_date = creation_date
        self._creation_time = creation_time

    @property
    def name(self) -> str:
        return self._name

    @property
    def born(self) -> datetime.datetime:
        return self._born

    @property
    def email(self) -> str:
        return self._email

    @property
    def creation_date(self) -> datetime.date:
        return self._creation_date

    @property
    def creation_time(self) -> datetime.time:
        return self._creation_time

    def __eq__(self, other: object):
        toret = False

        if isinstance(other, Person):
            toret = (self.name == other.name
                    and self.born == other.born
                    and self.email == other.email
                    and self.creation_date == other.creation_date
                    and self.creation_time == other.creation_time)
        
        return toret

    def __str__(self):
        return f"{self.name} ({self.born}): {self.email}"


class TestSirope(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._oid1 = sirope.OID(Person, 0)
        self._oid2 = sirope.OID(sirope.Sirope._get_full_name(Person), 1)
        self._p1 = Person("Baltasar",
                          datetime.datetime(1990, 1, 1),
                          "baltasarq@gmail.com",
                          datetime.datetime.now().date(),
                          datetime.datetime.now().time())
        self._p2 = Person("Rosa",
                          datetime.datetime(1984, 1, 1),
                          "zociguiguigui@gmail.com",
                          datetime.datetime.now().date(),
                          datetime.datetime.now().time())

        self._sirope = sirope.Sirope()

    def test_oid(self):
        self.assertEqual(sirope.Sirope._get_full_name(Person), self._oid1.namespace)
        self.assertEqual(0, self._oid1.num)
        self.assertEqual(sirope.Sirope._get_full_name(Person), self._oid2.namespace)
        self.assertEqual(1, self._oid2.num)

    def test_json(self):
        json_p1 = sirope.Sirope._json_from_obj(self._p1)
        obj_p1 = sirope.Sirope._obj_from_json(Person, json_p1)

        self.assertEqual(self._p1, obj_p1)

    def test_json_serialization(self):
        coder = sirope.JSONCoder()
        dcoder = sirope.JSONDCoder()

        s = coder.encode(self._p1.__dict__)
        d = dcoder.decode(s)
        obj_p1 = Person()
        obj_p1.__dict__ = d
        self.assertEqual(self._p1, obj_p1)

    def test_simple_save_restore(self):
        obj_p1 = None
        oid = self._oid1

        if self._sirope.exists(oid):
            self._sirope.delete(oid)
        
        oid = self._sirope.save(self._p1)
        obj_p1 = self._sirope.load(oid)

        self.assertTrue(self._sirope.exists(oid))
        self.assertEqual(self._p1, obj_p1)
        self.assertEqual(oid, obj_p1.oid)
        self.assertEqual(self._p1.oid, obj_p1.oid)


if __name__ == "__main__":
    unittest.main()
