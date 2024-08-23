import os
import json
import warnings
import logging
import sqlite3
from datetime import datetime
from treelib import Tree
import korus.tree as ktr
import korus.db as kdb 
import korus.db_util.table as ktb


class Taxonomy(ktr.KTree):
    """ Class for managing annotation taxonomies with a tree-like 
        structure where every child nodes has precisely one parent 
        node.

        The Taxonomy class is derived from korus.tax.KTree, 
        adding the following functionalities,

         * SQLite database storage
         * version tracking, including node ancestry       

        TODO: add assertion to check if there is an open connection to the sqlite database
        
        Args:
            name: str
                Short, descriptive name for the taxonomy.
            root_tag: str
                Tag for the root node. If specified, the root node will be 
                automatically created at initialisation.
            path: str
                Path to a SQLite database file for storing the taxonomy. 
                If None, the path will be set to ./{@name}.sqlite.
            overwrite: bool
                Whether to overwrite the file if it already exists.
    """
    def __init__(self, name="taxonomy", root_tag="root", path=None, overwrite=False):
        self.name = name
        self.version = None
        self.path = f"./{name}.sqlite" if path is None else path
        if os.path.exists(self.path) and overwrite:
            os.remove(self.path)

        super().__init__(root_tag=root_tag)

    @classmethod
    def from_dict(cls, input_dict, data_transform=None, path=None):
        """ Load a taxonomy from a dictionary.

            Expects the dictionary to have the keys 'name', 'version', 'tree'.

            Within the dictionary, the key 'children' is used to designate branching 
            points, and the key 'data' is used to designate any data associated with a 
            node.

            Args:
                input_dict: dict()
                    Input dictionary.
                data_transform: callable
                    This function gets applied to all entries in the dictionary with the key 'data'.
                path: str
                    Path to a SQLite database file for storing the taxonomy. 
            
            Returns:
                tax: korus.tax.Taxonomy
                    The taxonomy
        """
        tree_dict = input_dict.pop("tree")
        version = input_dict.pop("version")
        tax = ktr.tree_from_dict(cls(**input_dict, root_tag=None, path=path), recipe=tree_dict, data_transform=data_transform)
        tax.version = version
        return tax

    @classmethod
    def load(cls, path, name="taxonomy", version=None):
        """ Load an existing taxonomy from an SQLite database.

            The method expects to find the table,
            
                taxonomy(
                    id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    version TEXT,
                    tree JSON NOT NULL,
                    timestamp TEXT,
                    comment TEXT,
                    PRIMARY KEY (id),
                    UNIQUE (name, version)
                )
        
            Args:
                path: str
                    Path to the database file (.sqlite)
                name: str
                    Taxonomy name.
                version: int
                    Taxonomy version. If not specified, the latest version will be loaded.
                
            Returns:
                : korus.tax.Taxonomy
                    The taxonomy
        """
        conn = sqlite3.connect(path)
        
        query = f"SELECT name,version,tree FROM taxonomy WHERE name = '{name}'"
        if version is not None:
            query += f" AND version = '{version}'"
        
        c = conn.cursor()
        rows = c.execute(query).fetchall()

        conn.close()

        rows = sorted(rows, key=lambda x: x[1]) #sort according to version no.

        (name,version,data) = rows[-1]

        tax_dict = {"name":name, "version":version, "tree":json.loads(data)}
        return cls.from_dict(tax_dict, path=path)
    
    def latest_version(self):
        """ Get the latest version number"""
        if os.path.exists(self.path):
            conn = sqlite3.connect(self.path)
            c = conn.cursor()
            query = f"SELECT version FROM taxonomy WHERE name = '{self.name}'"
            rows = c.execute(query).fetchall()
            conn.close()
            version_numbers = [int(row[0]) for row in rows]
            if len(version_numbers) == 0:
                return None
            else:
                return max(version_numbers)

        else:
            return None

    def to_dict(self):
        """ Transform the taxonomy to a dictionary.
    
            Returns:
                : dict()
                    The taxonomy in the form of a dictionary.
        """
        return {"name":self.name, "version":self.version, "tree":ktr.tree_to_dict(self)}

    def save(self, comment=None, overwrite=False):
        """ Save the taxonomy.

            The version number is automatically incremented by +1 when this method is 
            called, unless @overwrite is set to True in which case the currently loaded 
            version is overwritten.

            Args:
                comment: str
                    Optional field. Typically used for describing the main changes made 
                    to the taxonomy since the last version.
                overwrite: bool
                    If True, the version no. is not incremented and instead the currently 
                    loaded version is overwritten.
        """
        v = self.latest_version()

        if v is None:
            self.version = 1 
        else:
            if not overwrite:
                self.version = v + 1

        self._to_sqlite(self.path, comment, overwrite)
        self.clear_history()

    def _to_sqlite(self, path, comment, overwrite=False):
        """ Save the taxonomy to an SQLite database.

            TODO: Implement @overwrite

            The database must contain the tables,

                taxonomy(
                    id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    tree JSON NOT NULL,
                    timestamp TEXT,
                    comment TEXT,
                    PRIMARY KEY (id),
                    UNIQUE (name, version)
                )

                taxonomy_created_node(
                    id TEXT NOT NULL,
                    precursor_id JSON NOT NULL,
                    is_equivalent INTEGER NOT NULL,
                    taxonomy_id INTEGER NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id)
                )

                taxonomy_removed_node(
                    id TEXT NOT NULL,
                    inheritor_id JSON NOT NULL,
                    is_equivalent INTEGER NOT NULL,
                    taxonomy_id INTEGER NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id)
                )
                
            If the database does not already exist or it does not 
            contain the required table, they will be automatically 
            created.

            Args:
                path: str
                    Path to the SQLite database file (*.sqlite)
                comment: str
                    Optional field. Typically used for describing the main changes made 
                    to the taxonomy since the last version.
                overwrite: bool
                    Set to True to allow existing entries in the taxonomy table with the 
                    same name and version no. to be overwritten.
        """
        db_exists = os.path.exists(path)

        # if folder does not exist, create it
        dir_path = os.path.dirname(path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        conn = sqlite3.connect(path) #will create the *.sqlite file if it does not already exist
        c = conn.cursor()

        # create the tables if they do not already exist
        if not db_exists:
            ktb.create_taxonomy_table(conn)
            ktb.create_taxonomy_created_node_table(conn)
            ktb.create_taxonomy_removed_node_table(conn)

        tax_dict = self.to_dict()

        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # check if taxonomy already exists
        name = tax_dict["name"]
        version = tax_dict["version"]
        rows = c.execute(f"SELECT id FROM taxonomy WHERE name = '{name}' AND version = {version}").fetchall()
        if len(rows) == 1:
            tax_exists = True
            tax_id = rows[0][0]
        else:
            tax_exists = False
            tax_id = None
            prev_comment = None

        if tax_exists and not overwrite:
            err_msg = f"Failed to save taxonomy."\
                + f" Database already contains a taxonomy named {name} with version no. {version}."\
                + f" Set @overwrite to True if you wish to overwrite the existing entry."
            logging.error(err_msg)
            return c

        elif tax_exists and overwrite:
            # retrieve comment
            (prev_comment,) = c.execute(f"SELECT comment FROM taxonomy WHERE id = {tax_id}").fetchall()[0]
            # delete existing entry
            c.execute(f"DELETE FROM taxonomy WHERE id = {tax_id}")

        # append to previous comment, if overwriting
        if prev_comment is not None:
            comment = prev_comment + "; " + comment

        # save taxonomy
        c.execute(
            "INSERT INTO taxonomy VALUES (?, ?, ?, ?, ?, ?)",
            [tax_id, tax_dict["name"], tax_dict["version"], json.dumps(tax_dict["tree"]), timestamp, comment]
        ) 

        if tax_id is None:
            tax_id = c.lastrowid 

        # save precursors of created nodes
        for created_id, (precursor_id, is_equivalent) in self.created_nodes.items():
            c.execute("INSERT INTO taxonomy_created_node VALUES (?, ?, ?, ?)", [created_id, json.dumps(precursor_id), int(is_equivalent), tax_id])

        # save inheritors of removed nodes
        for removed_id, (inheritor_id, is_equivalent) in self.removed_nodes.items():
            c.execute("INSERT INTO taxonomy_removed_node VALUES (?, ?, ?, ?)", [removed_id, json.dumps(inheritor_id), int(is_equivalent), tax_id])

        conn.commit()
        conn.close()


class AcousticTaxonomy(Taxonomy):
    """ Class for managing annotation acoustic taxonomies with a nested, 
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
            version: int
                Version number.
            overwrite: bool
                Whether to overwrite the file if it already exists.
    """
    def __init__(
            self, 
            name="acoustic_taxonomy", 
            root_tag="Unknown", 
            path=None,
            overwrite=False,
        ):
        self._type_root_tag = root_tag
        super().__init__(name=name, root_tag=root_tag, path=path, overwrite=overwrite)

    @classmethod
    def from_dict(cls, input_dict, path=None):
        """ Load an acoustic taxonomy from a dictionary.

            Overwrites Taxonomy.from_dict

            Expects the dictionary to have the keys 'name', 'version', 'tree'.

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
                    tree = ktr.KTree(root_tag=None)
                    tree = ktr.tree_from_dict(tree, types_dict)
                    x.update({"sound_types": tree})

            return x

        return super().from_dict(input_dict, data_transform, path=path)

    def _to_sqlite(self, path, comment=None, overwrite=False):
        """ Save the taxonomy to an SQLite database.

            Overwrites Taxonomy._to_sqlite

            The database must contain the tables,

                taxonomy(
                    id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    version TEXT,
                    tree JSON NOT NULL,
                    timestamp TEXT,
                    comment TEXT,
                    PRIMARY KEY (id),
                    UNIQUE (name, version)
                )

                taxonomy_created_node(
                    id TEXT NOT NULL,
                    precursor_id JSON NOT NULL,
                    is_equivalent INTEGER NOT NULL,
                    taxonomy_id INTEGER NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id)
                )

                taxonomy_removed_node(
                    id TEXT NOT NULL,
                    inheritor_id JSON NOT NULL,
                    is_equivalent INTEGER NOT NULL,
                    taxonomy_id INTEGER NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id)
                )
                
                label(
                    id INTEGER NOT NULL,
                    taxonomy_id INTEGER NOT NULL,
                    sound_source_tag TEXT,
                    sound_source_id TEXT,
                    sound_type_tag TEXT,
                    sound_type_id TEXT,
                    PRIMARY KEY (id),
                    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id),
                    UNIQUE (taxonomy_id, sound_source_uuid, sound_type_uuid)
                )

            If the database does not already exist or it does not 
            contain the required tables, they will be automatically 
            created.

            Args:
                path: str
                    Path to the SQLite database file (*.sqlite)
                comment: str
                    Optional field. Typically used for describing the main changes made 
                    to the taxonomy since the last version.
                overwrite: bool
                    Set to True to allow existing entries in the taxonomy table with the 
                    same name and version no. to be overwritten.
        """
        db_exists = os.path.exists(path)

        conn = sqlite3.connect(path) #will create the *.sqlite file if it does not already exist

        # create the tables if they do not already exist
        if not db_exists:
            ktb.create_taxonomy_table(conn)
            ktb.create_taxonomy_created_node_table(conn)
            ktb.create_taxonomy_removed_node_table(conn)
            ktb.create_label_table(conn)

        # insert data into tables
        kdb.insert_taxonomy(conn, self, comment=comment, overwrite=overwrite)

        conn.commit()
        conn.close()

    def create_node(self, tag, identifier=None, parent=None, precursor=None, inherit_types=True, **kwargs):
        """ Add a sound source to the taxonomy.

            Overwrites KTree.create_node.

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
            if inherit_types and self.root is not None: # inherit sound-type tree from parent            
                kwargs["sound_types"] = self.get_node(parent).data["sound_types"].deepcopy()
            
            else: # create empty sound-type tree            
                kwargs["sound_types"] = ktr.KTree(root_tag=self._type_root_tag)

        return super().create_node(tag=tag, identifier=identifier, parent=parent, precursor=precursor, **kwargs)

    def create_sound_source(self, tag, parent=None, precursor=None, inherit_types=True, **kwargs):
        """ Merely a wrapper for :meth:`create_node`"""
        return self.create_node(tag, parent=parent, precursor=precursor, inherit_types=inherit_types, **kwargs)
    
    def create_sound_type(self, tag, source_tag=None, parent=None, recursive=True, **kwargs):
        """ Add a sound type to the taxonomy.

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

    def merge_sound_sources(self, tag, children=None, remove=False, data_merge_fcn=None, inherit_types=True, **kwargs):
        """ Merge sound sources """
        return self.merge_nodes(tag, children=children, remove=remove, data_merge_fcn=data_merge_fcn, inherit_types=inherit_types, **kwargs)
    
    def merge_sound_types(self, tag, source_tag=None, children=None, remove=False, 
                          data_merge_fcn=None, recursive=True, **kwargs):
        """ Merge sound types """
        source_id = self.get_id(source_tag)

        if recursive:
            source_ids = self.expand_tree(nid=source_id, mode=Tree.DEPTH)
        else:
            source_ids = [source_id]

        for source_id in source_ids:
            types = self.get_node(source_id).data["sound_types"]
            types.merge_nodes(tag, children=children, remove=remove, data_merge_fcn=data_merge_fcn)

    def clear_history(self):
        """ Overwrites Taxonomy.clear_history
        """
        # commit changes for sound sources
        super().clear_history()

        # commit changes for sound types
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            self.get_node(source_id).data["sound_types"].clear_history()

    def sound_types(self, source_tag):
        """ Returns the KTree of sound types associated with a given sound source
        
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
    def created_nodes(self):
        """Overwrites KTree.created_nodes"""
        created_nodes = super().created_nodes
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            created_nodes.update(self.get_node(source_id).data["sound_types"].created_nodes)
        
        return created_nodes

    @property
    def removed_nodes(self):
        """Overwrites KTree.removed_nodes"""
        removed_nodes = super().removed_nodes
        for source_id in self.expand_tree(mode=Tree.DEPTH):
            removed_nodes.update(self.get_node(source_id).data["sound_types"].removed_nodes)
        
        return removed_nodes

    def ascend(self, source_tag, type_tag=None, include_start_node=True):
        """ Returns a python generator for ascending the taxonomy starting 
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
        types = self.get_node(source_tag).data["sound_types"]  #sound-type tree
        source_gen = self.rsearch(source_tag)  #ascending source-id generator

        counter = 0
        for sid in source_gen: #ascend up through sound sources
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
        """ Returns a python generator for descending the taxonomy starting 
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
                    raise ValueError(f"sound source '{source_i.tag}' does not have sound type '{type_tag}'")

                type_gen = types_i.expand_tree(types_i.get_id(type_tag), mode=Tree.DEPTH)
                for tid in type_gen:
                    type_i = types_i.get_node(tid)
                    if include_start_node or counter > 0:
                        yield source_i.tag, type_i.tag
                    counter += 1