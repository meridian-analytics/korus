from .interface import TableInterface

class DeploymentInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("deployment", backend)
