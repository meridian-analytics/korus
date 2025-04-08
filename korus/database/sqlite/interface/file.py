from korus.database.interface import FileInterface

class SQLiteFileInterface(FileInterface):
    def __init__(self):
        super().__init__()

    def filter(self):
        #search
        pass

    def get(self):
        #retrive entries
        pass

    def add(self):
        #add a new entry
        pass

    def __iter__(self):
        # iterates through the table rows
        pass

    def __str__(self) -> str:
        res = super().__str__()

         # TODO: add some summary stats of table contents
         # try: 
         # .mode column
         # .headers on
         # `pragma table_info(self.name)`
         
        return res