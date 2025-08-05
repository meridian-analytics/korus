import copy
from termcolor import colored

class Cursor():
    def __init__(self):
        self.history = []

    @property
    def current(self):
        if len(self) == 0:
            return None
        
        return self.history[-1]
    
    @property
    def previous(self):
        if len(self) <= 1:
            return None

        return self.history[-2]

    def __len__(self):
        return len(self.history)

    def __str__(self):
        if len(self) == 0:
            return ""

        h = copy.copy(self.history)
        h[-1] = colored(h[-1], "white", attrs=["bold"])
        h = "|".join(h)
        h = "[" + h + "] "
        return h

    def back(self):
        if len(self.history) == 0:
            return 
        
        del self.history[-1]
        return self

    def forward(self, name):
        self.history.append(name)
        return self


cursor = Cursor()