import json
import korus.db as kdb
from korus.util import list_to_str


def get_related_label_id(conn, label_id, ascend=False, descend=False, always_list=False):
    """ Get label identifiers of related nodes in the taxonomy tree.

        Args:
            conn: sqlite3.Connection
                Database connection
            label_id: int, list(int)
                Label identifiers
            ascend: bool
                Return the labels of ancestral nodes. 
            descend: bool
                Return the labels of descendant nodes. 
            always_list: bool
                Whether to always return a list. Default is False.
            
        Returns:
            related_id: int
                Identifiers of related nodes        
    """
    c = conn.cursor()

    related_id = []
    if not ascend and not descend:
        return related_id

    #loop over input label identifiers
    for _label_id in label_id if isinstance(label_id, list) else [label_id]: 

        # get source/type tags and taxonomy id
        query = f"SELECT sound_source_tag, sound_type_tag, taxonomy_id FROM label WHERE id = {_label_id}"
        (sound_source, sound_type, tax_id) = c.execute(query).fetchall()[0]          

        tax = kdb.get_taxonomy(conn, tax_id)

        if ascend:
            gen = tax.ascend(sound_source, sound_type, include_start_node=False)
            for i,(s,t) in enumerate(gen): #generator returns (source,type) tag tuple
                query = f"""
                    SELECT 
                        id 
                    FROM 
                        label 
                    WHERE 
                        sound_source_tag = '{s}'
                        AND sound_type_tag = '{t}'
                        AND taxonomy_id = {tax_id}
                    """
                (id,) = c.execute(query).fetchall()[0]
                related_id.append(id)

        if descend:
            gen = tax.descend(sound_source, sound_type, include_start_node=False)
            for i,(s,t) in enumerate(gen): #generator returns (source,type) tag tuple
                query = f"""
                    SELECT 
                        id 
                    FROM 
                        label 
                    WHERE 
                        sound_source_tag = '{s}'
                        AND sound_type_tag = '{t}'
                        AND taxonomy_id = {tax_id}
                    """
                (id,) = c.execute(query).fetchall()[0]
                related_id.append(id)

    if len(related_id) == 1 and not always_list:
        related_id = related_id[0]

    return related_id


def crosswalk_label_ids(conn, label_id, dst_taxonomy_id, ascend=False, descend=False, always_list=False, equiv=False):
    """ Map a list of label identifiers to another taxonomy.

        Same as @crosswalk_label_id, but for list inputs.

        Args:
            conn: sqlite3.Connection
                Database connection
            label_id: list(int)
                Label identifier
            dst_taxonomy_id: int
                Destination taxonomy identifier
            ascend: bool
                Also return the labels of ancestral nodes of the mapped node(s). 
            descend: bool
                Also return the labels of descendant nodes of the mapped node(s). 
            equiv: bool
                If True, only return the mapped label IDs that are 1-to-1

        Returns:
            mapped_label_id: list(str)
                The mapped label identifier(s).
    """
    mapped_label_ids = []
    for id in label_id:
        mapped_id, is_equiv = crosswalk_label_id(
            conn=conn, 
            label_id=id, 
            dst_taxonomy_id=dst_taxonomy_id, 
            ascend=ascend, 
            descend=descend, 
            always_list=True
        )
        if (equiv and is_equiv) or not equiv:
            mapped_label_ids += mapped_id

    return mapped_label_ids

def crosswalk_label_id(conn, label_id, dst_taxonomy_id, ascend=False, descend=False, always_list=False):
    """ Map a single label identifier to another taxonomy.

        Args:
            conn: sqlite3.Connection
                Database connection
            label_id: int
                Label identifier
            dst_taxonomy_id: int
                Destination taxonomy identifier
            ascend: bool
                Also return the labels of ancestral nodes of the mapped node(s). 
            descend: bool
                Also return the labels of descendant nodes of the mapped node(s). 
            always_list: bool
                Whether to always return a list. Default is False.

        Returns:
            mapped_label_id: list(str)
                The mapped label identifier(s).
            is_equivalent: bool
                Whether the input label and the mapped label(s) may be considered equivalent. 
    """
    c = conn.cursor()

    # special case: (None, None)
    query = f"SELECT sound_source_tag,sound_type_tag FROM label WHERE id = {label_id}"
    (ss, st) = c.execute(query).fetchall()[0]
    if ss is None and st is None:
        query = f"""
            SELECT 
                id 
            FROM 
                label 
            WHERE 
                sound_source_tag IS NULL 
                AND sound_type_tag IS NULL 
                AND taxonomy_id = {dst_taxonomy_id}
        """
        (mapped_label_id,) = c.execute(query).fetchall()[0]
        if always_list:
            mapped_label_id = [mapped_label_id]

        return mapped_label_id, True


    # origin taxonomy id
    query = f"SELECT taxonomy_id FROM label WHERE id = '{label_id}'"
    rows = c.execute(query).fetchall()
    (org,) = rows[0]

    # destination taxonomy
    dst = dst_taxonomy_id

    if org == dst:
        mapped_label_id = [label_id]
        mapped_label_id += get_related_label_id(conn, mapped_label_id, descend=descend, ascend=ascend, always_list=True)
        if not always_list and len(mapped_label_id) == 1:
            mapped_label_id = mapped_label_id[0]
        return mapped_label_id, True
        
    # sound source and sound type UUIDs in origin taxonomy
    query = f"SELECT sound_source_id, sound_type_id FROM label WHERE id = '{label_id}'"
    rows = c.execute(query).fetchall()
    source_id, type_id = rows[0]

    # check if the same UUID tuple exists in the dst taxonomy
    query = f"""
        SELECT 
            id
        FROM 
            label 
        WHERE 
            sound_source_id = '{source_id}' 
            AND sound_type_id = '{type_id}' 
            AND taxonomy_id = '{dst}'
        """
    rows = c.execute(query).fetchall()
    mapped_label_id = [row[0] for row in rows]
    if len(rows) == 1:
        mapped_label_id += get_related_label_id(conn, mapped_label_id, descend=descend, ascend=ascend, always_list=True)
        if not always_list:
            mapped_label_id = mapped_label_id[0]
        return mapped_label_id, True
    
    # forward or backward mode
    mode = "b" if dst < org else "f"

    # trace node history
    mapped_source_id, source_is_equivalent = trace_node_history(conn, source_id, dst, mode)
    mapped_type_id, type_is_equivalent = trace_node_history(conn, type_id, dst, mode)

    is_equivalent = source_is_equivalent and type_is_equivalent

    # finally, query label identifier using the mapped UUIDs
    mapped_source_id_str = list_to_str(mapped_source_id)  
    mapped_type_id_str = list_to_str(mapped_type_id) 
    query = f"""
        SELECT 
            id
        FROM 
            label 
        WHERE 
            sound_source_id IN {mapped_source_id_str} 
            AND sound_type_id IN {mapped_type_id_str}  
            AND taxonomy_id = '{dst}'
        """
    rows = c.execute(query).fetchall()
    mapped_label_id = [row[0] for row in rows]
    mapped_label_id += get_related_label_id(conn, mapped_label_id, descend=descend, ascend=ascend, always_list=True)

    if not always_list and len(mapped_label_id) == 1:
        mapped_label_id = mapped_label_id[0]

    return mapped_label_id, is_equivalent


def crosswalk_label(conn, source_type, origin_taxonomy_id, dst_taxonomy_id, ascend=False, descend=False, always_list=False):
    """ Map a label (sound-source, sound-type tag tuple) to another taxonomy.

        Wrapper for :func:`crosswalk_label_id`

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple(str, str)
                Sound-source, sound-type tag tuple. 
            origin_taxonomy_id: int
                Origin taxonomy identifier
            dst_taxonomy_id: int
                Destination taxonomy identifier
            ascend: bool
                Also return the labels of ancestral nodes of the mapped node(s). 
            descend: bool
                Also return the labels of descendant nodes of the mapped node(s). 
            always_list: bool
                Whether to always return a list. Default is False.

        Returns:
            mapped_label: list(tuple)
                The mapped label(s).
            is_equivalent: bool
                Whether the input label and the mapped label(s) may be considered equivalent. 
    """
    c = conn.cursor()

    org = origin_taxonomy_id
    dst = dst_taxonomy_id

    # map tag tuple -> label_id in origin taxonomy
    (source_tag, type_tag) = source_type
    query = f"""
        SELECT 
            id
        FROM 
            label 
        WHERE 
            sound_source_tag = '{source_tag}' 
            AND sound_type_tag = '{type_tag}' 
            AND taxonomy_id = '{org}'
        """
    rows = c.execute(query).fetchall()
    (label_id, ) = rows[0]

    mapped_label_id, is_equivalent = crosswalk_label_id(
        conn, 
        label_id, 
        dst_taxonomy_id=dst, 
        always_list=always_list,
        ascend=ascend,
        descend=descend
    )

    # map label_id -> tag tuple
    mapped_label_id_str = list_to_str(mapped_label_id)
    query = f"""
        SELECT 
            sound_source_tag, sound_type_tag
        FROM 
            label 
        WHERE 
            id IN {mapped_label_id_str} 
        """
    mapped_label = c.execute(query).fetchall()

    return mapped_label, is_equivalent


def trace_node_history(conn, node_id, dst_taxonomy_id, mode="backward"):
    """ Maps a taxonomy node to an earlier taxonomy version. 

        Args:
            conn: sqlite3.Connection
                Database connection
            node_id: str
                Sound source or sound type UUID.
            dst_taxonomy_id: int
                Destination taxonomy identifier
            mode: str
                * backward/b: trace node history backwards in time (default)
                * forward/f: trace node history forward in time

        Returns:
            mapped_ids: list(str)
                UUID(s) in the destination taxonomy.
            is_equivalent: bool
                Whether the input node and the mapped node(s) may be considered equivalent.             
    """
    mode = mode.lower()[0]
    assert mode == "b" or mode == "f", "mode must be either backward (b) or forward (f)"

    if mode == "b":
        col_name = "precursor_id"
        table_name = "taxonomy_created_node" 
    else:
        col_name = "inheritor_id"
        table_name = "taxonomy_removed_node"

    dst = dst_taxonomy_id
    ids = [node_id]    
    mapped_ids = []

    c = conn.cursor()

    is_equivalent = True

    while True:
        for id in ids:
            query = f"""
                SELECT 
                    id 
                FROM 
                    label 
                WHERE 
                    (sound_source_id = '{id}' OR sound_type_id = '{id}') 
                    AND taxonomy_id = '{dst}'
            """
            rows = c.execute(query).fetchall()
            if len(rows) > 0:
                mapped_ids.append(id)

        ids = [x for x in ids if x not in mapped_ids]

        if len(ids) == 0:
            break

        ids_str = list_to_str(ids)  #"(" + ",".join([f"'{x}'" for x in ids]) + ")"
        query = f"""
            SELECT 
                {col_name}, is_equivalent
            FROM 
                {table_name}
            WHERE 
                id IN {ids_str}
            """
        rows = c.execute(query).fetchall()

        ids = []
        for row in rows:
            (precursor_id, equiv) = row
            ids += json.loads(precursor_id)
            if not equiv:
                is_equivalent = False

    return mapped_ids, is_equivalent
