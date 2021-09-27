import os

VERSION = "0.0.0"
DATABASE = os.environ.get("NODLITE_DB", "db/graph.db")
TTL = os.environ.get("NODLITE_TTL", 10)
