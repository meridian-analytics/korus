from .interface import TableInterface

class DeploymentInterface(TableInterface):
    def __init__(self):
        super().__init__("deployment")
