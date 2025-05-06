from copy import deepcopy


class Taxonomy:
    def __init__(self, **kwargs):
        self.version = kwargs.get("version", None)
        self.timestamp = kwargs.get("timestamp", None)
        self.changes = kwargs.get("changes", [])
        self.created_nodes = kwargs.get("created_nodes", {})
        self.removed_nodes = kwargs.get("removed_nodes", {})
        self.comment = kwargs.get("comment", None)

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)

    def to_dict(self):
        return {
            "version": self.version,
            "timestamp": self.timestamp,
            "changes": self.changes,
            "created_nodes": self.created_nodes,
            "removed_nodes": self.removed_nodes,
            "comment": self.comment,
            "tree": dict(),
        }

    def deepcopy(self):
        return deepcopy(self)
