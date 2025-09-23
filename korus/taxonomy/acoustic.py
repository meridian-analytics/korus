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
    def all_labels(self) -> list[tuple]:
        """Tags and identifiers of all the nodes in the taxonomy.

        Returns:
            : list[tuple]
                Each item is a tuple of the form (sound-source tag, sound-type tag, sound-source identifer, sound-type identifier)
        """
        _labels = []
        for sound_source in self.all_nodes_itr():
            for sound_type in sound_source.data["sound_types"].all_nodes_itr():
                _labels.append(
                    (
                        sound_source.tag,
                        sound_type.tag,
                        sound_source.identifier,
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

    def sound_types(self, source_tag: str) -> Taxonomy:
        """Returns the taxonomy of sound types associated with a given sound source

        Args:
            source_tag: str
                Sound source tag or identifier

        Returns:
            t: Taxonomy
                Sound types. Returns None if the sound source does not exist.
        """
        try:
            return self.get_node(source_tag).data["sound_types"]
        except:
            return None

    def label_exists(self, source_tag: str, type_tag: str = None) -> bool:
        """Check if certain (source,type) label exists in the taxonomy.

        Args:
            source_tag: str
                Sound-source tag or identifier of starting node.
            type_tag: str
                Sound-type tag or identifier of starting node.

        Returns:
            exists: bool
                True if label exists, False otherwise.
        """
        types = self.sound_types(source_tag)
        if types is None:
            return False

        elif type_tag is None:
            return True

        else:
            return types.get_node(type_tag) is not None

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
        """Returns a python generator for ascending the taxonomy starting at the specified node.

        If the algorithm encounters an ancestral sound source the sound-type tree of which
        does not contain the specified sound type, it will ascend the sound-type tree of the
        starting node until it finds a common sound type from where it can start iterating.

        Args:
            source_tag: str
                Sound-source tag or identifier of starting node. If None or '*', an empty
                iterator is returned.
            type_tag: str
                Sound-type tag or identifier of starting node. If None or '*', the
                generator only iterates through the sound-source nodes.
            include_start_node: bool
                Whether to include the starting node. Default is True.

        Yields:
            source_tag, type_tag: str, str
        """
        if source_tag is None or source_tag == "*":
            return iter(())

        # sound types of starting node
        start_types = self.get_node(source_tag).data["sound_types"]

        # iterate over ancestors
        counter = 0
        for sid in self.rsearch(source_tag):
            source = self.get_node(sid)

            if type_tag is None or type_tag == "*":
                if include_start_node or counter > 0:
                    yield source.tag, type_tag

                counter += 1

            else:
                # sound types of ancestor
                types = self.sound_types(sid)

                # iterate over sound types of starting node until a common sound type is found
                for tid in start_types.rsearch(type_tag):
                    start_tag = start_types.get_node(tid).tag
                    if types.get_node(start_tag) is not None:
                        type_iter = types.rsearch(start_tag)
                        break

                # iterator over sound types in ancestor
                for tid in type_iter:
                    if include_start_node or counter > 0:
                        yield source.tag, types.get_node(tid).tag

                    counter += 1

    def descend(self, source_tag, type_tag=None, include_start_node=True):
        """Returns a python generator for descending the taxonomy starting at the specified node.

        If the algorithm encounters a descendant sound source the sound-type tree of which
        does not contain the starting sound type, it will skip the descendant altogether.

        Args:
            source_tag: str
                Sound-source tag or identifier of starting node.
            type_tag: str
                Sound-type tag or identifier of starting node. If None or '*', the
                generator will only iterate through the sound-source nodes.
            include_start_node: bool
                Whether to include the starting node. Default is True.

        Yields:
            source_tag, type_tag: str, str
        """
        if source_tag is None or source_tag == "*":
            return iter(())

        # iterate over descendant nodes
        counter = 0
        for sid in self.expand_tree(self.get_id(source_tag), mode=Tree.DEPTH):
            source = self.get_node(sid)

            if type_tag is None or type_tag == "*":
                if include_start_node or counter > 0:
                    yield source.tag, type_tag

                counter += 1

            else:
                # sound types of the descendant
                types = self.sound_types(sid)

                # if the descendant does not have the starting sound type, skip it
                if types.get_node(type_tag) is None:
                    continue

                # create an iterator over the descendant's sound types
                type_iter = types.expand_tree(types.get_id(type_tag), mode=Tree.DEPTH)

                # iterate over the sound types
                for tid in type_iter:
                    if include_start_node or counter > 0:
                        yield source.tag, types.get_node(tid).tag

                    counter += 1

    def last_common_ancestor(self, labels: list[tuple[str, str]]):
        """Finds the last common ancestor of a set of labels.

        Args:
            labels: list[tuple[str,str]]
                List of labels. Each label is a tuple of the form (sound-source tag, sound-type tag)
                or (sound-source identifier, sound-type identifier)

        Returns:
            : tuple[str, str]
                The label of the last common ancestor, which may be one of the input labels

        Raises:
            AssertionError: if one of the labels does not exist in the taxonomy
        """
        # last common sound-source ancestor
        source_tags = [label[0] for label in labels]
        common_source_tag = super().last_common_ancestor(source_tags)

        # for each sound source, iterate over sound types until a common sound type is found with the common source
        type_tags = []
        for label in labels:
            assert_msg = f"Taxonomy does not have label {label}"
            assert self.label_exists(*label), assert_msg

            source_tag, type_tag = label
            types = self.sound_types(source_tag)

            for id in types.rsearch(type_tag):
                tag = types.get_node(id).tag
                if self.label_exists(common_source_tag, tag):
                    break

            type_tags.append(tag)

        # last common sound-type ancestor
        common_types = self.sound_types(common_source_tag)
        common_type_tag = common_types.last_common_ancestor(type_tags)

        return (common_source_tag, common_type_tag)
