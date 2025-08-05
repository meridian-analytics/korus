import copy
from termcolor import colored

class Cursor():
    def __init__(self):
        self.history = []

    @property
    def id(self):
        if len(self) == 0:
            return None
        
        return self.history[-1][0]

    def __len__(self):
        return len(self.history)

    def __str__(self):
        if len(self) == 0:
            return ""

        names = [h[1] for h in self.history]
        names[-1] = colored(names[-1], "white", attrs=["bold"])
        names = "|".join(names)
        names = "[" + names + "] "
        return names

    def back(self):
        if len(self.history) == 0:
            return 
        
        del self.history[-1]
        return self

    def to(self, node):
        self.history.append((node.id, node.name))
        return self


cursor = Cursor()