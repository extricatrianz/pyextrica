from sqlalchemy.dialects import registry
from .util import _url as URL


registry.register("pyextrica", "pyextrica.sqlalchemy.dialect", "TrinoDialect")

