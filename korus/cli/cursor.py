import copy
from termcolor import colored


class Cursor:
    def __init__(self):
        self.history = []
        self._item = None

    @property
    def item(self) -> str:
        return self._item
    
    @item.setter
    def item(self, i: str):
        self._item = i

    @property
    def module(self):
        return None if len(self) == 0 else self.history[-1]

    @property
    def id(self):
        return None if self.module is None else self.module.id

    def __len__(self):
        return len(self.history)

    def __str__(self):
        if len(self) == 0:
            return ""

        names = [module.name for module in self.history]

        if self.item is not None:
            names.append(self.item)

        names[-1] = colored(names[-1], "white", attrs=["bold"])
        names = "|".join(names)
        names = "[" + names + "] "
        return names

    def go_back(self):
        self.item = None
        if len(self.history) > 0:
            del self.history[-1]

    def go_to(self, module):
        self.history.append(module)
        self.item = None

    def execute(self):
        new_id = self.module()
        if new_id is None:
            self.go_back()
            new_id = self.module.id
            self.go_back()

        return new_id
        


cursor = Cursor()
