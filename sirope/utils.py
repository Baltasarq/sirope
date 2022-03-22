# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import sys


def cls_from_str(path: str) -> type:
    """Returns the class from its full name."""
    path_parts = path.split('.')
    cls_name = path_parts.pop()
    mdl_name = ".".join(path_parts)
    mdl = sys.modules.get(mdl_name)
    return mdl.__dict__.get(cls_name) if mdl else None


def full_name_from_obj(o: object) -> str:
    """Returns the full qualified name of the class of this object."""
    cl = o.__class__ if not isinstance(o, type) else o
    module = cl.__module__
    cls_name = cl.__name__

    if module is None or module == str.__class__.__module__:
        return cls_name

    return module + '.' + cls_name
