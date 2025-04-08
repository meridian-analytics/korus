from korus.database.sqlite import SQLiteDatabase

def database(backend="sqlite", **kwargs):
    if backend == "sqlite":
        return SQLiteDatabase(**kwargs)


