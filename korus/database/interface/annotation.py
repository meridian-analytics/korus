from .interface import TableInterface

class AnnotationInterface(TableInterface):
    def __init__(self):
        super().__init__("annotation")