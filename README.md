# Sirope

**Sirope** es una pequeña librería que envuelve la librería cliente de [Redis para Python](https://pypi.org/project/redis/).
[Redis](https://redis.io/) es un almacenamiento de pares clave-valor. Mediante el almacenamiento denominado *hash*, se guardan los objetos que
pertenecen a una determinada clase usando JSON. Utilizado de esta forma, tenemos una base de datos [NoSql](https://es.wikipedia.org/wiki/NoSQL).

**Sirope** is a thin wrapper around the [Redis for Python](https://pypi.org/project/redis/) library. [Redis](https://redis.io/) is a store of
key-value pairs. Employing the store type called *hash*, objects pertaining to a given class are saved together as JSON. Used this way, we obtain a
[NoSql](https://en.wikipedia.org/wiki/NoSQL) database.

```
# Save object p1
p1 = Person("Baltasar")
sirope = sirope.Sirope()
oid = sirope.save(p1)
```

```
# Retrieve object p1
restpred_p1 = Person()
sirope = sirope.Sirope()
sirope.load(oid, restored_p1)
print(restored_p1)
```

