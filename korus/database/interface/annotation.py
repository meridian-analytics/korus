from .interface import TableInterface

class AnnotationInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("annotation", backend)

    def get(self):
        """TODO: handle conversion to different formats: raven,ketos"""
        pass