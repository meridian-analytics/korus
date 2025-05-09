from treelib import Tree
from .taxonomy import Taxonomy, tree_from_dict


class AcousticTaxonomy(Taxonomy):
    """Class for managing annotation acoustic taxonomies with a nested,
    tree-like structure.

    When annotating acoustic data, it is customary to describe sounds
    by their source (e.g. a killer whale) as well as their type, i.e,
    their aural and spectral characteristics (e.g. a tonal call).

    In the AcousticTaxonomy class, the nodes of the (primary) tree are
    the sound sources, and nested within each of these node is a
    (secondary) tree of sound types.

    Args:
        name: str
            Descriptive, short name for the taxonomy.
        root_tag: str
            Tag for the root node. If specified, the root node will be
            automatically created at initialisation.
    """

    def __init__(
        self,
        name="acoustic_taxonomy",
        root_tag="Unknown",
        **kwargs,
    ):
        self._type_root_tag = root_tag
        super().__init__(name=name, root_tag=root_tag, **kwargs)

    @property
    def all_labels(self):
        _labels = []
        for sound_source in self.all_nodes_itr():
            for sound_type in sound_source.data["sound_types"].all_nodes_itr():
                _labels.append(
                    (
                        sound_source.tag,
                        sound_source.identifier,
                        sound_type.tag,
                        sound_type.identifier,
                    )
                )

        return _labels

    @classmethod
    def from_dict(cls, input_dict):
        """Load an acoustic taxonomy from a dictionary.

        Overwrites Taxonomy.from_dict

        Expects the dictionary to have the keys

            tree, name, version, changes, timestamp, comment, created_nodes, removed_nodes

        Within the dictionary, the key 'children' is used to designate branching
        points, and the key 'data' is used to designate any data associated with a
        node. The key 'types' is used to designate a sub-tree of sound types
        associated with a particular node.

        Args:
            input_dict: dict()
                Input dictionary.
            path: str
                Path to a SQLite database file for storing the taxonomy.
        """

        def data_transform(x):
            if x is not None:
                types_dict = x.pop("sound_types", None)
                if types_dict is not None:
                    tax = Taxonomy(name="sound_types", root_tag=None)
                    tax = tree_from_dict(tax, types_dict)
                    x.update({"sound_types": tax})

            return x

        return super().from_dict(input_dict, data_transform)

    def create_node(
        self,
        tag,
        identifier=None,
        parent=None,
        precursor=None,
        inherit_types=True,
        **kwargs,
    ):
        """Add a sound source to the taxonomy.

        Overwrites Taxonomy.create_node

        Sound source attributes can be specified using keyword arguments.

        It is recommended to include the following attributes:

            * name
            * description
            * scientific_name
            * tsn

        Args:
            tag: str
                Tag for the sound source.
            parent: str
                Parent sound source identifier or tag. The default value is None, implying 'root' as parent.
            precursor: str, list(str)
                Used for tracking the ancestry of the child node. If None, the parent identifier will be used.
            inherit_types: bool
                Inherit sound types from parent source. Default is True.

        Returns:
            node: treelib.node.Node
                The new node object

        Raises:
            AssertionError: if the taxonomy already contains a sound source with the specified tag
        """
        if parent is None:
            parent = self.root

        if kwargs is None or len(kwargs) == 0:
            kwargs = dict()

        if "sound_types" not in kwargs:
            if (
                inherit_types and self.root is not None
            ):  # inherit sound-type tree from parent
                tax = self.get_node(parent).data["sound_types"].deepcopy()
                tax.name = f"{tag}_sound_types"
                kwargs["sound_types"] = tax

            else:  # create empty sound-type tree
                kwargs["sound_types"] = Taxonomy(
                    name=f"{tag}_sound_types", root_tag=self._type_root_tag
                )

        return super().create_node(
            tag=tag, identifier=identifier, parent=parent, precursor=precursor, **kwargs
        )

    def create_sound_source(
        self, tag, parent=None, precursor=None, inherit_types=True, **kwargs
    ):
        """Merely a wrapper for :meth:`create_node`"""
        return self.create_node(
            tag,
            parent=parent,
            precursor=precursor,
            inherit_types=inherit_types,
            **kwargs,
        )

    def create_sound_type(
        self, tag, source_tag=None, parent=None, recursive=True, **kwargs
    ):
        """Add a sound type to the taxonomy.

        Note that the sound type must be associated with a particular sound source, i.e.,
        a particular node in the primary tree (which can be the root node).

        Also, note that if @recursive is set to true, all child nodes (sound sources)
        will inherit the sound type.

        Keyword arguments can be used to specify additional data to be associated
        with the sound type, e.g., a wordy description of its acoustic characteristics.

        Args:
            tag: str
                Tag for the sound type. Must be unique within the sound source.
            source_tag: str
                Tag of the sound source that the sound type is to be associated with.
            parent: str
                Tag or identifier of the parent sound type. Use this to create a hierarchy
                of sound types.
            recursive: bool
                Also add this sound type to all descendant sound sources. Default is True.
        """
        source_id = self.get_id(source_tag)

        if recursive:
            source_ids = self.expand_tree(nid=source_id, mode=Tree.DEPTH)
        else:
            source_ids = [source_id]

        # loop over sound sources
        for source_id in source_ids:
            n = self.get_node(source_id)
            types = n.data["sound_types"]

            if parent is None:
                parent_tag = types.root
            else:
                parent_tag = parent

            parent_id = types.get_id(parent_tag)

            if kwargs is None or len(kwargs) == 0:
                kwargs = dict()

            # add the new sound type to the KTree
            types.create_node(tag=tag, parent=parent_id, **kwargs)

    def merge_sound_sources(
        self,
        tag,
        children=None,
        remove=False,
        data_merge_fcn=None,
        inherit_types=True,
        **kwargs,
    ):
        """Merge sound sources"""
        return self.merge_nodes(
            tag,
            children=children,
            remove=remove,
            data_merge_fcn=data_merge_fcn,
            inherit_types=inherit_types,
            **kwargs,
        )

    def merge_sound_types(
        self,
        tag,
        source_tag=None,
        children=None,
        remove=False,
        data_merge_fcn=None,
        recursive=True,
        **kwargs,
    ):
        """Merge sound types"""
        source_id = self.get_id(source_tag)

        if recursive:
            source_ids = self.expand_tree(nid=source_id, mode=Tree.DEPTH)
        else:
            source_ids = [source_id]

        for source_id in source_ids:
            types = self.get_node(source_id).data["sound_types"]
            types.merge_nodes(
                tag, children=children, remove=remove, data_merge_fcn=data_merge_fcn
            )

    def clear_history(self):
        """Overwrites Taxonomy.clear_history"""
        # commit changes for sound sources
        super().clear_history()

        # commit changes for sound types
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            self.get_node(source_id).data["sound_types"].clear_history()

    def sound_types(self, source_tag):
        """Returns the KTree of sound types associated with a given sound source

        Args:
            source_tag: str
                Sound source tag

        Returns:
            t: korus.tree.KTree
                Sound types. Returns None if the sound source does not exist.
        """
        try:
            return self.get_node(source_tag).data["sound_types"]
        except:
            return None

    @property
    def changes(self):
        """Overwrites Taxonomy.changes"""
        changes = super().changes
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            changes += self.get_node(source_id).data["sound_types"].changes

        return changes

    @property
    def created_nodes(self):
        """Overwrites Taxonomy.created_nodes"""
        created_nodes = super().created_nodes
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            created_nodes.update(
                self.get_node(source_id).data["sound_types"].created_nodes
            )

        return created_nodes

    @property
    def removed_nodes(self):
        """Overwrites Taxonomy.removed_nodes"""
        removed_nodes = super().removed_nodes
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            removed_nodes.update(
                self.get_node(source_id).data["sound_types"].removed_nodes
            )

        return removed_nodes

    def ascend(self, source_tag, type_tag=None, include_start_node=True):
        """Returns a python generator for ascending the taxonomy starting
        at @source_tag, @type_tag.

        Args:
            source_tag: str
                Sound source tag or identifier of starting node.
            type_tag: str
                Sound type tag of starting node. If None or '%', the
                generator will only iterate through the sound-source nodes.
            include_start_node: bool
                Whether to include the starting node. Default is True.

        Yields:
            source_tag, type_tag: str, str
        """
        # debug_msg = f"[{self.__class__.__name__}] Ascending {self.name} v{self.version} starting from ({source_tag},{type_tag})"
        # logging.debug(debug_msg)

        types = self.get_node(source_tag).data["sound_types"]  # sound-type tree
        source_gen = self.rsearch(source_tag)  # ascending source-id generator

        counter = 0
        for sid in source_gen:  # ascend up through sound sources
            source_i = self.get_node(sid)
            types_i = source_i.data["sound_types"]

            if type_tag is None or type_tag == "%":
                if include_start_node or counter > 0:
                    yield source_i.tag, type_tag

                counter += 1

            else:
                for tid in types.rsearch(type_tag):
                    if types_i.get_node(tid) is not None:
                        type_gen = types_i.rsearch(tid)
                        break

                for tid in type_gen:
                    type_i = types_i.get_node(tid)
                    if include_start_node or counter > 0:
                        yield source_i.tag, type_i.tag

                    counter += 1

    def descend(self, source_tag, type_tag=None, include_start_node=True):
        """Returns a python generator for descending the taxonomy starting
        at @source_tag, @type_tag.

        Args:
            source_tag: str
                Sound source tag or identifier of starting node.
            type_tag: str
                Sound type tag of starting node. If None or '%', the
                generator will only iterate through the sound-source nodes.
            include_start_node: bool
                Whether to include the starting node. Default is True.

        Yields:
            source_tag, type_tag: str, str
        """
        # debug_msg = f"[{self.__class__.__name__}] Descending {self.name} v{self.version} starting from ({source_tag},{type_tag})"
        # logging.debug(debug_msg)

        source_gen = self.expand_tree(self.get_id(source_tag), mode=Tree.DEPTH)

        counter = 0
        for sid in source_gen:
            source_i = self.get_node(sid)
            types_i = source_i.data["sound_types"]

            if type_tag is None or type_tag == "%":
                if include_start_node or counter > 0:
                    yield source_i.tag, type_tag

                counter += 1

            else:
                if types_i.get_node(type_tag) is None:
                    # debug_msg = f"[{self.__class__.__name__}] Sound source '{source_i.tag}' does not have sound type '{type_tag}'. Skipping ..."
                    # logging.debug(debug_msg)
                    continue

                type_gen = types_i.expand_tree(
                    types_i.get_id(type_tag), mode=Tree.DEPTH
                )
                for tid in type_gen:
                    type_i = types_i.get_node(tid)
                    if include_start_node or counter > 0:
                        yield source_i.tag, type_i.tag

                    counter += 1
