# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import unittest
import sirope


class Person:
    def __init__(self, name: str="", age: int=0, email: str=""):
        self._name = name
        self._age = age
        self._email = email

    @property
    def name(self):
        return self._name

    @property
    def age(self):
        return self._age

    @property
    def email(self):
        return self._email

    def __eq__(self, other: object):
        toret = False

        if isinstance(other, Person):
            toret = (self.name == other.name
                    and self.age == self.age
                    and self.email == self.email)
        
        return toret

    def __str__(self):
        return f"{self.name} ({self.age}): {self.email}"


class TestSirope(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._oid1 = sirope.OID(Person, 0)
        self._oid2 = sirope.OID("Person", 1)
        self._p1 = Person("Baltasar", 47, "baltasarq@gmail.com")
        self._p2 = Person("Rosa", 41, "zociguiguigui@gmail.com")
        self._sirope = sirope.Sirope()

    def test_oid(self):
        self.assertEqual(Person.__name__, self._oid1.namespace)
        self.assertEqual(1, self._oid1.num)
        self.assertEqual("Person", self._oid2.namespace)
        self.assertEqual(2, self._oid2.num)

    def test_json(self):
        json_p1 = sirope.Sirope._json_from_obj(self._p1)
        obj_p1 = sirope.Sirope._obj_from_json(Person, json_p1)

        self.assertEqual('{"_name": "Baltasar", "_age": 47, "_email": "baltasarq@gmail.com"}', json_p1)
        self.assertEqual(self._p1, obj_p1)

    def test_simple_save_restore(self):
        obj_p1 = None
        oid = self._oid1

        if not self._sirope.exists(oid):
            oid = self._sirope.save(self._p1)

        self._sirope.load(oid, obj_p1)

        self.assertTrue(self._sirope.exists(oid))
        self.assertEqual(self._p1, obj_p1)
        self.assertEqual(oid, obj_p1.oid)
        self.assertEqual(self._p1.oid, obj_p1.oid)


if __name__ == "__main__":
    unittest.main()
