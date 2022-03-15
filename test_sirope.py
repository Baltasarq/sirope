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
        self._oid2 = sirope.OID(Person, 1)
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

    def tearDown(self) -> None:
        super().tearDown()

        if not self._sirope.exists(self._oid1):
            self._sirope.delete(self._oid1)

        if not self._sirope.exists(self._oid2):
            self._sirope.delete(self._oid2)

    def test_oid(self):
        self.assertEqual(sirope.Sirope._get_full_name(Person), self._oid1.namespace)
        self.assertEqual(0, self._oid1.num)
        self.assertEqual(sirope.Sirope._get_full_name(Person), self._oid2.namespace)
        self.assertEqual(1, self._oid2.num)
        
        obj_oid = sirope.OID.from_dict({"namespace": "__main__.Person", "num": 0})
        self.assertEqual(obj_oid, self._oid1)

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

    def test_exists(self):
        if self._sirope.exists(self._oid1):
            self._sirope.delete(self._oid1)

        self.assertFalse(self._sirope.exists(self._oid1))

    def test_simple_save_restore(self):
        obj_p1 = None
        oid = self._oid1

        if self._sirope.exists(oid):
            self._sirope.delete(oid)

        if self._sirope.exists(self._oid2):
            self._sirope.delete(self._oid2)
        
        oid = self._sirope.save(self._p1)
        obj_p1 = self._sirope.load(oid)

        self.assertEqual(oid, self._oid1)
        self.assertEqual(oid, self._p1.__oid__)
        self.assertEqual(oid, obj_p1.__oid__)
        self.assertEqual(self._p1.__oid__, obj_p1.__oid__)
        self.assertTrue(self._sirope.exists(oid))
        self.assertEqual(self._p1, obj_p1)

        self._sirope.save(self._p1)
        self.assertTrue(self._sirope.exists(self._oid1))
        self.assertEqual(1, self._sirope.num_objs_for(Person))
        self._sirope.delete(self._oid1)

    def test_load_all(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs_for(Person))
        
        list_persons = self._sirope.load_all_of(Person)
        self.assertEqual(2, len(list_persons))
        self.assertEqual(self._oid1, list_persons[0].__oid__)
        self.assertEqual(self._oid2, list_persons[1].__oid__)

        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_multi_delete(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)
        
        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs_for(Person))

        self._sirope.multi_delete([self._oid1, self._oid2])
        self.assertEqual(0, self._sirope.num_objs_for(Person))

    def test_multi_load(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs_for(Person))

        lps = self._sirope.load_multi_of(Person, [self._oid1, self._oid2])

        self.assertEqual(2, len(lps))
        self.assertEqual(self._p1, lps[0])
        self.assertEqual(self._p2, lps[1])
        
        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_load_all_oids(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs_for(Person))

        loids = self._sirope.load_all_keys_of(Person)
        
        self.assertEqual(2, len(loids))
        self.assertTrue(self._oid1 in loids)
        self.assertTrue(self._oid2 in loids)
        
        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)


if __name__ == "__main__":
    unittest.main()
