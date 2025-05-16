import copy
from treelib import Tree
from datetime import datetime


def tree_from_dict(tree, recipe, parent=None, data_transform=None):
    """Transform a dictionary into a tree.

    Within the dictionary, the key 'children' is used to designate branching
    points. The key 'data' is used to designate any data associated with a
    node.

    Args:
        tree: korus.tree.LabelTree
            Parent tree to which the data will be appended. Can be an empty tree.
        recipe: dict
            Dictionary recipe for building the tree.
        parent: str
            Tag or identifier of the node within the parent tree where the data will
            be appended. By default parent=None implying that the data should be appended
            at the root level.
        data_transform: callable
            This function gets applied to all entries in the dictionary with the
            key 'data'.

    Returns:
        tree: Tree
            The created tree.
    """
    if data_transform is None:
        data_transform = lambda x: x

    branch_key = "children"

    for key, value in recipe.items():
        if "data" in value.keys():
            data = data_transform(value["data"])
        else:
            data = None

        nid = value["id"]
        tree.create_node(tag=key, identifier=nid, parent=parent, **data)

        if branch_key in value.keys() and isinstance(value[branch_key], dict):
            tree = tree_from_dict(tree, value[branch_key], nid, data_transform)

    tree.clear_history()
    return tree


def tree_to_dict(tree, nid=None, key=None, sort=True, reverse=False):
    """Transform a tree into a dictionary.

    The key 'children' is used to designate branching points.

    Args:
        tree: korus.tree.LabelTree
            Input tree.
        nid: str
            Only transform the part of the tree below this node.
            Default is root.
        key: str
             @key and @reverse are present to sort nodes at each level.
             If @key is None sorting is performed on node tag.
        sort: bool
            Whether to sort nodes. The default value is True.
        reverse: bool
            if True, reverse sorting

    Returns:
        tree_dict: dict()
            The created dictionary.
    """
    tree = Tree(tree, deep=True)  # make a deep copy

    branch_key = "children"

    nid = tree.root if (nid is None) else nid
    ntag = tree[nid].tag
    tree_dict = {ntag: {branch_key: {}}}

    def parse_data_attr(x):
        if x is not None and isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, Tree):
                    x[k] = tree_to_dict(tree=v, sort=sort, reverse=reverse)
        return x

    tree_dict[ntag]["id"] = nid
    tree_dict[ntag]["data"] = parse_data_attr(tree[nid].data)

    if tree[nid].expanded:
        queue = [tree[i] for i in tree[nid].successors(tree._identifier)]
        key = (lambda x: x) if (key is None) else key

        if sort:
            queue.sort(key=key, reverse=reverse)

        for elem in queue:
            tree_dict[ntag][branch_key].update(
                tree_to_dict(tree, elem.identifier, sort=sort, reverse=reverse)
            )

        if len(tree_dict[ntag][branch_key]) == 0:
            tree_dict = {ntag: {"id": nid, "data": parse_data_attr(tree[nid].data)}}

        return tree_dict


class Taxonomy(Tree):
    """Class for managing annotation taxonomies with a tree-like structure
    where every child nodes has precisely one parent node.

    Derived from the treelib.tree.Tree class. It adds a few new features
    and makes a few changes:

     * tags are required to be unique.
     * UUIDs are always used as identifiers.
     * sibling nodes can be merged with the `merge_nodes` method.
     * the node creation/removal history (precursor/heritor) is tracked.
     * a root node may be automatically created at construction time.
     * changes must be committed before certain operations are allowed,
       e.g., removing or moving a newly created node.

    Args:
        name: str
            Short, descriptive name for the taxonomy.
        root_tag: str
            Tag for the root node. If specified, the root node will be automatically created at initialisation.

    Attrs & Properties:
        name:
        version:
        timestamp:
        comment:
        changes:
        created_nodes:
            maps created_node_uuid -> (precursor_node_uuid(s), is_equivalent)
        removed_nodes:
            maps removed_node_uuid -> (inheritor_node_uuid(s), is_equivalent)
    """

    def __init__(
        self,
        name: str = "taxonomy",
        root_tag: str = "root",
        version: int = None,
        timestamp: datetime = None,
        changes: list[str] = None,
        comment: str = None,
        created_nodes: dict = None,
        removed_nodes: dict = None,
    ):
        super().__init__()

        self.name = name

        self.version = version
        self.timestamp = timestamp
        self.comment = comment
        self._changes = [] if changes is None else changes
        self._created_nodes = {} if created_nodes is None else created_nodes
        self._removed_nodes = {} if removed_nodes is None else removed_nodes

        self._tag_to_id = {}
        self._data_merge_fcn = lambda x: dict()

        if root_tag is not None:
            self.create_node(root_tag)

    @property
    def changes(self):
        return self._changes

    @changes.setter
    def changes(self, x):
        self._changes = x

    @property
    def created_nodes(self):
        return self._created_nodes

    @created_nodes.setter
    def created_nodes(self, x):
        self._created_nodes = x

    @property
    def removed_nodes(self):
        return self._removed_nodes

    @removed_nodes.setter
    def removed_nodes(self, x):
        self._removed_nodes = x

    @property
    def all_labels(self):
        return [(node.tag, node.identifier) for node in self.all_nodes_itr()]

    @classmethod
    def from_dict(cls, input_dict, data_transform=None):
        """Load a taxonomy from a dictionary.

        Expects the dictionary to have the keys,

            tree, name, version, changes, timestamp, comment, created_nodes, removed_nodes

        Within the 'tree' dictionary, the key 'children' is used to designate branching
        points, and the key 'data' is used to designate any data associated with a
        node.

        Args:
            input_dict: dict()
                Input dictionary.
            data_transform: callable
                This function gets applied to all entries in the dictionary with the key 'data'.

        Returns:
            tax: korus.taxonomy.Taxonomy
                The taxonomy
        """
        recipe = input_dict.pop("tree")
        tax = tree_from_dict(
            cls(**input_dict, root_tag=None),
            recipe=recipe,
            data_transform=data_transform,
        )
        return tax

    def to_dict(self) -> dict:
        """Transform the taxonomy to a dictionary.

        Returns:
            : dict
                The taxonomy in the form of a dictionary.
        """
        return {
            "tree": tree_to_dict(self),
            "name": self.name,
            "version": self.version,
            "timestamp": self.timestamp,
            "comment": self.comment,
            "changes": self.changes,
            "created_nodes": self.created_nodes,
            "removed_nodes": self.removed_nodes,
        }

    def deepcopy(self):
        """Make a deep copy of the present instance
        See https://docs.python.org/2/library/copy.html
        """
        return copy.deepcopy(self)

    def clear_history(self):
        """Clear the history of created and removed nodes."""
        self.version = None
        self.timestamp = None
        self.comment = None
        self._changes = []
        self._created_nodes = {}
        self._removed_nodes = {}

    def show(self, append_name=False, **kwargs):
        """Overwrites treelib.tree.Tree.show.

        Args:
            name: bool
                Whether to append the node 'name' to the tag. Default is False.
        """
        if not append_name:
            print(self)

        else:
            tree = Tree(self, deep=True)  # make a deep copy
            for n in tree.all_nodes_itr():
                if n.data is None:
                    continue

                if "name" in n.data:
                    n.tag += f' [{n.data["name"]}]'

            print(tree)

    def _create_tag_to_id_mapping(self):
        """Create tag -> identifier mapping

        Returns:
            : dict
                Dictionary mapping tags to identifiers
        """
        return {self.get_node(id).tag: id for id in self.expand_tree(mode=Tree.DEPTH)}

    def get_node(self, n):
        """Overwrites treelib.tree.Tree.get_node

        Args:
            n: str
                Node tag or identifier
        """
        return super().get_node(self.get_id(n))

    def rsearch(self, n, filter=None):
        """Overwrites treelib.tree.Tree.rsearch

        Args:
            n: str
                Node tag or identifier
            filter: callable
                Function of one variable to act on the Node object
        """
        return super().rsearch(self.get_id(n), filter)

    def get_id(self, tag):
        """Filters the tree nodes based on their tags.

        Note: If a valid identifier is specified as input, it will be returned unchanged.

        Args:
            tag: str,int,list,tuple
                The selected tag(s)

        Returns:
            ids: str,list
                The corresponding node identifier(s).
        """
        if tag is None:
            return None

        is_list = isinstance(tag, (list, tuple))
        tag_list = tag if is_list else [tag]

        id_list = []
        for tag in tag_list:
            if tag in self._tag_to_id.keys():  # if input is a tag, map to id
                id_list.append(self._tag_to_id[tag])
            elif (
                tag in self._tag_to_id.values()
            ):  # if input is already an id, do nothing
                id_list.append(tag)
            else:  # else, return None
                id_list.append(None)

        if is_list:
            return id_list
        else:
            return id_list[0]

    def create_node(self, tag, identifier=None, parent=None, precursor=None, **kwargs):
        """Create a new, child node for a parent node.

        Node attributes can be specified using keyword arguments.

        Args:
            tag: str
                Child node tag.
            identifier: str
                Child node identifier. If absent, a UUID will be generated automatically.
            parent: str
                Parent node identifier or tag. The default value is None, implying 'root' as parent.
            precursor: tuple
                (IDs, is_equivalent) tuple, used for tracking the ancestry of the child node.
                If None, the parent identifier will be used.

        Returns:
            node: treelib.node.Node
                The new node object

        Raises:
            AssertionError: if the tree already contains a node with the specified tag
        """
        assert (
            self.get_id(tag) is None
        ), f"tree already contains a node with the tag '{tag}'"

        if kwargs is None or len(kwargs) == 0:
            kwargs = dict()

        parent = self.get_id(parent)

        node = super().create_node(
            tag=tag, identifier=identifier, parent=parent, data=kwargs
        )

        self._created_nodes[node.identifier] = (
            ([self.get_id(parent)], False) if precursor is None else precursor
        )
        self._tag_to_id[tag] = node.identifier

        self._changes.append(f"{self.name}: Added {tag}")

        return node

    def merge_nodes(
        self, tag, children=None, remove=False, data_merge_fcn=None, **kwargs
    ):
        """Create a new node by merging two or more existing nodes.

        Only sibling nodes can be merged in this manner.

        Use keyword arguments to specify attributes of the merged node.

        Args:
            tag: str
                Tag of the new, merged node.
            children: list(str)
                Identifiers or tags of the nodes to be merged. These will become the
                children nodes of the new, merged node.
            remove: bool
                Remove source nodes after they have been merged. Default is False.
            data_merge_fcn: callable
                Receives as input a list of the 'data' attributes of the source nodes
                and returns the 'data' attribute of the merged node.

        Returns:
            node: treelib.node.Node
                The new node object

        Raises:
            AssertionError: if the tree already contains a node with the specified tag
            AssertionError: if the source nodes are not siblings
        """
        self._detect_new_node(children)

        if data_merge_fcn is None:
            data_merge_fcn = self._data_merge_fcn

        children = self.get_id(children)  # tag -> identifier

        # check that nodes are siblings (have the same parent)
        parent_nodes = [self.parent(id) for id in children]
        for parent_node in parent_nodes:
            assert (
                parent_node.identifier == parent_nodes[0].identifier
            ), "children must be siblings to merge"

        if kwargs is None or len(kwargs) == 0:
            kwargs = dict()

        # merge node data
        kwargs.update(data_merge_fcn([self.get_node(id).data for id in children]))

        # create new, merged node
        node = self.create_node(
            tag=tag, parent=parent_node.tag, precursor=(children, True), **kwargs
        )

        # change message
        children_tags = ", ".join([self.get_node(id).tag for id in children])
        msg = f"{self.name}: Merged {children_tags} -> {tag} with remove={remove}"
        self._changes.append(msg)

        # move or remove original nodes
        for id in children:
            self.move_node(
                id, node.identifier
            )  # move before removing, to ensure correct ancestry history
            if remove:
                self.remove_node(id)

        return node

    def remove_node(self, n):
        """Overwrites treelib.tree.Tree.remove_node

        Args:
            n: str
                Node tag or identifier
        """
        self._detect_new_node(n)

        n = self.get_id(n)  # tag -> identifier

        # change message
        self._changes.append(f"{self.name}: Removed {self.get_node(n).tag}")

        # set inheritors of removed nodes
        parent_id = self.parent(n).identifier
        node_gen = self.expand_tree(n, mode=Tree.DEPTH)
        for nid in node_gen:
            self._removed_nodes[nid] = ([parent_id], False)  # (IDs, is_equivalent)

        return super().remove_node(n)  # remove node and all nodes below it

    def move_node(self, n, new_parent):
        """Overwrites treelib.tree.Tree.move_node

        Args:
            n: str
                Node tag or identifier
        """
        self._detect_new_node(n)
        n = self.get_id(n)  # tag -> identifier
        new_parent = self.get_id(new_parent)

        msg = f"{self.name}: Moved {self.get_node(n).tag} from {self.parent(n).tag} -> {self.get_node(new_parent).tag}"
        self._changes.append(msg)

        return super().move_node(n, new_parent)

    def link_past_node(self, n):
        """Overwrites treelib.tree.Tree.link_past_node

        Args:
            n: str
                Node tag or identifier
        """
        self._detect_new_node(n)
        n = self.get_id(n)  # tag -> identifier

        # set inheritors of removed nodes
        self._removed_nodes[n] = (
            [child.identifier for child in self.children(n)],
            True,
        )  # (IDs, is_equivalent)

        msg = f"{self.name}: Linked past {self.get_node(n).tag}"
        self._changes.append(msg)

        return super().link_past_node(n)

    def is_ancestor(self, ancestor, grandchild):
        """Overwrites treelib.tree.Tree.is_ancestor

        Args:
            ancestor: str
                Node tag or identifier
            grandchild: str
                Node tag or identifier
        """
        ancestor = self.get_id(ancestor)
        grandchild = self.get_id(grandchild)
        return super().is_ancestor(ancestor, grandchild)

    def _detect_new_node(self, n):
        """Check if a given node is new.

        Args:
            n: str, list(str)
                Node tag or identifier

        Raises:
            RuntimeError: if any of the nodes are new
        """
        n_list = n if isinstance(n, (list, tuple)) else [n]
        for n in n_list:
            n = self.get_id(n)
            if n in self.created_nodes:
                raise RuntimeError(
                    "New node detected. Call clear_history before proceeding."
                )

    def last_common_ancestor(self, x):
        """Finds the last common ancestor of a set of nodes

        Args:
            x: list(str)
                List of node tags or identifiers

        Returns:
            : str
                The tag of the last common ancestor, which may be one of the input nodes
        """
        nids = [self.get_id(n) for n in x]

        # find the shallowest node
        level_min = self.level(nids[0])
        nid_shallowest = nids[0]
        for nid in nids[1:]:
            l = self.level(nid)
            if l < level_min:
                nid_shallowest = nid
                level_min = l

        # ascend the tree, at each step checking
        # if we have found a common ancestor
        for nid_lca in self.rsearch(nid_shallowest):
            is_lca = True
            for nid in nids:
                if not self.is_ancestor(nid_lca, nid) and nid_lca != nid:
                    is_lca = False
                    break

            if is_lca:
                return self.get_node(nid_lca).tag

    def ascend(self, n, include_start_node=True):
        """Returns a python generator for ascending the taxonomy starting at a given node.

        Args:
            n: str
                Node tag or identifier
            include_start_node: bool
                Whether to include the starting node. Default is True.

        Yields:
            tag: tuple[str]
        """
        if not include_start_node:
            if self.get_node(n).is_root():
                return iter(())

            n = self.parent(self.get_id(n)).identifier

        for n in self.rsearch(n):
            yield (self.get_node(n).tag,)

    def descend(self, n, include_start_node=True):
        """Returns a python generator for descending the taxonomy starting at a given node.

        Args:
            n: str
                Node tag or identifier.
            include_start_node: bool
                Whether to include the starting node. Default is True.

        Yields:
            tag: tuple[str]
        """
        if not include_start_node and self.get_node(n).is_leaf():
            return iter(())

        counter = 0
        for n in self.expand_tree(self.get_id(n), mode=Tree.DEPTH):
            if counter > 0:
                yield (self.get_node(n).tag,)

            counter += 1
