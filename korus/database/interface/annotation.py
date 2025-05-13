from .interface import TableInterface


class AnnotationInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("annotation", backend)

    def get(self):
        """TODO: handle conversion to different formats: raven,ketos"""
        pass

    def filter(self, spatiotemporal_cut: callable):
        """TODO: 
        
        spatiotemporal_cut:  fcn(t,lat,lon,z) -> bool
        """
        pass