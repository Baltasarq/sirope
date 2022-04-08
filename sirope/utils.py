# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import sys
from typing import Any


def cls_from_str(path: str) -> Any:
    """Returns the class from its full name."""
    if not path:
        raise ValueError("cls_from_str(): path invalid")

    if path:
        path_parts = path.split('.')
        cls_name = path_parts.pop()
        mdl_name = ".".join(path_parts)
        mdl = sys.modules.get(mdl_name)
        toret = mdl.__dict__.get(cls_name) if mdl else None

    return toret

def full_name_from_obj(o: object) -> str:
    """Returns the full qualified name of the class of this object."""
    if not o:
        raise ValueError("full_name_from_obj(): invalid object")

    if o:
        cl = o.__class__ if not isinstance(o, type) else o
        module = cl.__module__
        cls_name = cl.__name__

        if module is None or module == str.__class__.__module__:
            toret = cls_name
        else:
            toret = module + '.' + cls_name

    return toret
