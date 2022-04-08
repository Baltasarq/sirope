# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import unittest

import sirope
import datetime


class Person:
    def __init__(self,
                 name: str, born: datetime.datetime, email: str,
                 creation_date: datetime.date,
                 creation_time: datetime.time):
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

"""
class TestSafeIndex(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._redis = Redis()
        self._sirope = sirope.Sirope()
        self._ndx = sirope.SafeIndex(self._redis)
        self._oid1 = sirope.OID(Person, 0)
        self._oid2 = sirope.OID(Person, 1)

    def tearDown(self) -> None:
        super().tearDown()

        if self._ndx.exists_for(self._oid1):
            self._ndx.delete_for(self._oid1)

        if self._ndx.exists_for(self._oid2):
            self._ndx.delete_for(self._oid2)

    def test_basics(self):
        soid1 = self._ndx.build_for(self._oid1)
        soid2 = self._ndx.build_for(self._oid1)
        soid3 = self._ndx.build_for(self._oid2)
        soid4 = self._ndx.build_for(self._oid2)

        self.assertEqual(2, len(self._ndx))
        self.assertEqual(soid1, soid2)
        self.assertEqual(soid3, soid4)
        self.assertEqual(soid1, self._ndx.exists_for(self._oid1))
        self.assertEqual(soid3, self._ndx.exists_for(self._oid2))

        self._ndx.delete_for(self._oid1)
        self._ndx.delete_for(self._oid2)

    def test_exists(self):
        self.assertEqual(None, self._ndx.exists_for(self._oid1))
        soid1 = self._ndx.build_for(self._oid1)

        self.assertEqual(1, len(self._ndx))
        self.assertEqual(soid1, self._ndx.exists_for(self._oid1))

        self._ndx.delete_for(self._oid1)
        self.assertEqual(0, len(self._ndx))

    def test_sirope_basics(self):
        soid1 = self._sirope.build_safe_for_oid(self._oid1)
        soid2 = self._sirope.build_safe_for_oid(self._oid1)
        soid3 = self._sirope.build_safe_for_oid(self._oid2)
        soid4 = self._sirope.build_safe_for_oid(self._oid2)

        self.assertEqual(2, len(self._ndx))
        self.assertEqual(soid1, soid2)
        self.assertEqual(soid3, soid4)
        self.assertEqual(self._oid1, self._sirope.get_oid_from_safe(soid1))
        self.assertEqual(self._oid2, self._sirope.get_oid_from_safe(soid3))

        self._ndx.delete_for(self._oid1)
        self._ndx.delete_for(self._oid2)
"""


class TestSirope(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._oid1 = sirope.OID(Person, 0)
        self._oid2 = sirope.OID(Person, 1)
        self._p1 = Person("Baltasar",
                          datetime.datetime(1970, 1, 1),
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

        if self._sirope.exists(self._oid1):
            self._sirope.delete(self._oid1)

        if self._sirope.exists(self._oid2):
            self._sirope.delete(self._oid2)

    def test_oid(self):
        self.assertTrue(self._oid1.namespace.endswith(Person.__name__))
        self.assertEqual(0, self._oid1.num)
        self.assertTrue(self._oid2.namespace.endswith(Person.__name__))
        self.assertEqual(1, self._oid2.num)
        
        obj_oid = sirope.OID.from_pair(("__main__.Person", 0))
        self.assertEqual(obj_oid, self._oid1)
    """
    def test_json(self):
        json_p1 = sirope.main_class._json_from_obj(self._p1)
        obj_p1 = sirope.main_class._obj_from_json(Person, json_p1)

        self.assertEqual(self._p1, obj_p1)
    """

    """
    def test_json_serialization(self):
        coder = sirope.JSONCoder()
        dcoder = sirope.JSONDCoder()

        s = coder.encode(self._p1.__dict__)
        d = dcoder.decode(s)
        obj_p1 = Person()
        obj_p1.__dict__ = d
        self.assertEqual(self._p1, obj_p1)
    """

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
        self.assertEqual(1, self._sirope.num_objs(Person))
        self._sirope.delete(self._oid1)

    def test_multiple_saves_same_object(self):
        if self._sirope.exists(self._oid1):
            self._sirope.delete(self._oid1)
        
        oid1 = self._sirope.save(self._p1)
        self.assertEqual(1, self._sirope.num_objs(Person))

        oid2 = self._sirope.save(self._p1)
        oid3 = self._sirope.save(self._p1)
        oid4 = self._sirope.save(self._p1)
        oid5 = self._sirope.save(self._p1)

        self.assertEqual(1, self._sirope.num_objs(Person))
        self.assertEqual(self._oid1, oid1)
        self.assertEqual(self._oid1, oid2)
        self.assertEqual(self._oid1, oid3)
        self.assertEqual(self._oid1, oid4)
        self.assertEqual(self._oid1, oid5)

        self._sirope.multi_delete([oid1, oid2, oid3, oid4, oid5])

    def test_load_all(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs(Person))
        
        list_persons = list(self._sirope.load_all(Person))
        self.assertEqual(2, len(list_persons))
        loids = [p.__oid__ for p in list_persons]
        self.assertTrue(self._oid1 in loids)
        self.assertTrue(self._oid2 in loids)

        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_multi_delete(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)
        
        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs(Person))

        self._sirope.multi_delete([self._oid1, self._oid2])
        self.assertEqual(0, self._sirope.num_objs(Person))

    def test_multi_load(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs(Person))

        lps = list(self._sirope.multi_load([self._oid1, self._oid2]))

        self.assertEqual(2, len(lps))
        self.assertTrue(self._p1 in lps)
        self.assertTrue(self._p2 in lps)
        
        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_load_all_oids(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs(Person))

        loids = list(self._sirope.load_all_keys(Person))
        
        self.assertEqual(2, len(loids))
        self.assertTrue(self._oid1 in loids)
        self.assertTrue(self._oid2 in loids)
        
        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_filter(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs(Person))

        fl1 = list(self._sirope.filter(Person, lambda p: p.born.year == 1970))
        fl2 = list(self._sirope.filter(Person, lambda p: p.name == "Rosa"))
        
        self.assertEqual(1, len(fl1))
        self.assertEqual(1, len(fl2))

        self.assertEqual(self._p1, fl1[0])
        self.assertEqual(self._p2, fl2[0])

        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_filter_max(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        self.assertEqual(2, self._sirope.num_objs(Person))

        fl1 = list(self._sirope.filter(Person, lambda p: p.born.year >= 1970, max=1))
        fl2 = list(self._sirope.filter(Person, lambda p: p.born.year >= 1970))
        
        self.assertEqual(1, len(fl1))
        self.assertEqual(2, len(fl2))

        self.assertTrue(self._p2 in fl2)
        self.assertTrue(self._p1 == fl1[0] or self._p2 == fl1[0])

        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

    def test_enumerate(self):
        lps = [(self._oid1, self._p1), (self._oid2, self._p2)]

        for pp in lps:
            if not self._sirope.exists(pp[0]):
                self._sirope.save(pp[1])

        self.assertEqual(len(lps), self._sirope.num_objs(Person))

        persons = [pair[1] for pair in lps]
        for i, p in enumerate(self._sirope.enumerate(Person)):
            self.assertTrue(p in persons)
            lps[i] = (lps[i][0], p)

        for p in lps:
            self._sirope.delete(p[1].__oid__)

        lps.clear()

    def test_enumerate_max(self):
        lps = [(self._oid1, self._p1), (self._oid2, self._p2)]

        for pp in lps:
            if not self._sirope.exists(pp[0]):
                self._sirope.save(pp[1])

        self.assertEqual(len(lps), self._sirope.num_objs(Person))

        persons = [pair[1] for pair in lps]
        num = 0
        for i, p in enumerate(self._sirope.enumerate(Person, 1)):
            self.assertTrue(p in persons)
            lps[i] = (lps[i][0], p)
            num += 1

        self.assertEqual(1, num)

        for p in lps:
            self._sirope.delete(p[0])

        lps.clear()

    def test_find_first(self):
        lps = [(self._oid1, self._p1), (self._oid2, self._p2)]

        for pp in lps:
            if not self._sirope.exists(pp[0]):
                self._sirope.save(pp[1])

        self.assertEqual(len(lps), self._sirope.num_objs(Person))

        self.assertEqual(lps[0][1],
                         self._sirope.find_first(Person, lambda p: p.name == "Baltasar"))

        for p in lps:
            self._sirope.delete(p[1].__oid__)

        lps.clear()

    def test_load_first(self):
        lps = [(self._oid1, self._p1), (self._oid2, self._p2)]

        for pp in lps:
            if not self._sirope.exists(pp[0]):
                self._sirope.save(pp[1])

        self.assertEqual(len(lps), self._sirope.num_objs(Person))
        lf1 = list(self._sirope.load_first(Person, 1))
        self.assertEqual(1, len(lf1))
        self.assertEqual(lf1[0], self._p1)

        for p in lps:
            self._sirope.delete(p[1].__oid__)

        lps.clear()

    def test_load_last(self):
        lps = [(self._oid1, self._p1), (self._oid2, self._p2)]

        for pp in lps:
            if not self._sirope.exists(pp[0]):
                self._sirope.save(pp[1])

        self.assertEqual(len(lps), self._sirope.num_objs(Person))
        ll1 = list(self._sirope.load_last(Person, 1))
        self.assertEqual(1, len(ll1))
        self.assertEqual(ll1[0], self._p2)

        for p in lps:
            self._sirope.delete(p[1].__oid__)

        lps.clear()

    def test_persistent_indexes(self):
        if not self._sirope.exists(self._oid1):
            self._sirope.save(self._p1)

        if not self._sirope.exists(self._oid2):
            self._sirope.save(self._p2)

        soid1 = self._sirope.safe_from_oid(self._oid1)
        soid2 = self._sirope.safe_from_oid(self._oid2)

        self.assertEqual(2, self._sirope.num_safe_indexes())

        poid1 = self._sirope.oid_from_safe(soid1)
        poid2 = self._sirope.oid_from_safe(soid2)

        self.assertEqual(self._oid1, poid1)
        self.assertEqual(self._oid2, poid2)

        self._sirope.delete(self._oid1)
        self._sirope.delete(self._oid2)

        self.assertEqual(0, self._sirope.num_safe_indexes())
        self.assertEqual(0, self._sirope.num_objs(Person))


if __name__ == "__main__":
    unittest.main()
