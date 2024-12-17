
import os
import json
import sqlite3
import logging
import warnings
import traceback
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta
from treelib import Tree
import traceback
from korus.util import get_num_samples_and_rate, list_to_str
import korus.tax as kx
import korus.db_util.table as ktb
import korus.db_util.label as klb
from korus.app.app_util.add import edit_row_manually


def filter_files(conn, deployment_id=None, start_utc=None, end_utc=None, job_id=None):
    """ Search for the files in the database based on deployment and time range
    
    Args:
        conn: 
            Database connection
        deployment_id: int
            Deployment identifier
        start_utc: datetime.datetime
            UTC start time
        end_utc: datetime.datetime
            UTC end time
        job_id: int
            Job identifier

    Returns:
        ids: list(int)
            File identifiers matching the search criteria
    """
    where_cond = []

    if deployment_id is not None:
        where_cond.append(f"deployment_id = {deployment_id}")

    if end_utc is not None:
        end_str = end_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        #where_cond.append(f"start_utc - '{end_str}' <= 0.")
        where_cond.append(f"JULIANDAY(start_utc) - JULIANDAY('{end_str}') <= 0.")
    
    if start_utc is not None:
        start_str = start_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        #where_cond.append(f"JULIANDAY(start_utc) + num_samples / sample_rate / 24. / 3600. - JULIANDAY('{start_str}') > 0.")
        #where_cond.append(f"end_utc - '{start_str}' >= 0.")
        where_cond.append(f"JULIANDAY(end_utc) - JULIANDAY('{start_str}') >= 0.")

    if len(where_cond) > 0:
        where_cond = " WHERE " + " AND ".join(where_cond)
    else:
        where_cond = ""

    c = conn.cursor()
    query = f"SELECT id FROM file{where_cond}"
    rows = c.execute(query).fetchall()
    file_ids = [row[0] for row in rows]

    if job_id is not None:
        query = f"SELECT file_id FROM file_job_relation WHERE job_id = {job_id}"
        rows = c.execute(query).fetchall()
        file_ids_job = [row[0] for row in rows]
        file_ids = list(set(file_ids) & set(file_ids_job)) #lists intersection

    return file_ids
    

def filter_annotation(
        conn, 
        source_type=None,
        exclude=None,
        tag=None,
        granularity=None,
        invert=False,
        strict=False,
        tentative=False,
        ambiguous=False,
        file=False,
        valid=False,
        taxonomy_id=None,
        job_id=None,
        deployment_id=None,
    ):
    """ Query annotation table by filtering on sound source and sound type.

        TODO: implement strict
        TODO: implement file
        TODO: implement valid
        TODO: consider renaming `source_type` to `select`

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple, list(tuple)                
                Select annotations with this (source,type) label.
                The character '%' can be used as wildcard.
                Accepts both a single tuple and a list of tuples.
                By default all descendant nodes in the taxonomy tree are also considered. Use 
                the @strict argument to change this behaviour.
            exclude: tuple, list(tuple)
                Select annotations with this (source,type) *exclusion label* while also 
                excluding annotations with this (source,type) label.
                The character '%' can be used as wildcard.
                Accepts both a single tuple and a list of tuples.
                By default all descendant nodes in the taxonomy tree are also considered. Use 
                the @strict argument to change this behaviour.
            tag: str,list(str)
                Select annotations with this tag.
            granularity: str, list(str)
                Annotation granularity. Options are 'unit', 'window', 'file', 'batch', 'encounter'.
            invert: bool
                Invert the label filtering criteria so that annotations with the (source,type) 
                specified by the @source_type argument are excluded rather than selected.
                The character '%' can be used as wildcard.
                Accepts both a single tuple and a list of tuples.
                By default both ancestral and descendant nodes in the taxonomy tree are considered 
                when performing an inverted search. Use the @strict argument to change this behaviour.
            strict: bool
                Whether to interpret labels 'strictly', meaning that ancestral/descendant nodes 
                in the taxonomy tree are not considered. For example, when filtering on 'KW' 
                annotations labelled as 'SRKW' will *not* be selected if @strict is set to True. 
                Default is False. 
            tentative: bool
                Whether to filter on tentative label assignments, when available. Default is False.
            ambiguous: bool
                Whether to also filter on ambiguous label assignments. Default is False.
            file: bool
                If True, exclude annotations pertaining to audio files not present in the database. 
                Default is False. NOT YET IMPLEMENTED.
            valid: bool
                If True, exclude annotations with invalid data or flagged as requiring review.  
                Default is False. NOT YET IMPLEMENTED.
            taxonomy_id: int
                Acoustic taxonomy that the (source,type) label arguments refer to. If not specified, 
                the latest taxonomy will be used.
            job_id: int, list(int)
                Restrict search to the specified annotation job(s).
            deployment_id: int, list(int) 
                Restrict search to the specified deployment(s).

        Returns:
            indices: list(int)
                Annotation indices  
    """
    if strict:
        raise NotImplementedError("`strict` not yet implemented")

    if file:
        raise NotImplementedError("`file` not yet implemented")

    if valid:
        raise NotImplementedError("`valid` not yet implemented")

    c = conn.cursor()

    where_conditions = []

    if job_id is not None:
        wc = f"WHERE a.job_id IN {list_to_str(job_id)}"
        where_conditions.append(wc)
        
    if deployment_id is not None:
        wc = f"WHERE a.deployment_id IN {list_to_str(deployment_id)}"
        where_conditions.append(wc)

    if source_type is not None:
        # @source_type
        if not invert:
            wc = _select_label_condition(conn, source_type, strict, tentative, ambiguous, taxonomy_id)
            where_conditions.append(wc)

            if exclude is not None:
                wc = _exclude_label_condition(conn, exclude, strict, tentative, ambiguous, taxonomy_id)
                where_conditions.append(wc)

        # @source_type with @invert=True
        else:
            wc = _invert_select_label_condition(conn, source_type, strict, tentative, ambiguous, taxonomy_id)
            where_conditions.append(wc)

    # @tag
    if tag is not None:
        rows = c.execute(f"SELECT id FROM tag WHERE name IN {list_to_str(tag)}").fetchall()
        tag_ids = [str(row[0]) for row in rows]
        tag_ids_str = ",".join(tag_ids)
        wc = f"tag_id.value IN ({tag_ids_str})"
        where_conditions.append(wc)

    # exclude auto-generated negatives (unless explicitly included in tag search)
    if tag is None or ktb.AUTO_NEG not in tag:
        (auto_neg_id,) = c.execute(f"SELECT id FROM tag WHERE name = '{ktb.AUTO_NEG}'").fetchall()[0]
        wc = f"(tag_id.value != {auto_neg_id} OR tag_id.value IS NULL)"
        where_conditions.append(wc)

    # @granularity
    if granularity is not None:
        rows = c.execute(f"SELECT id FROM granularity WHERE name IN {list_to_str(granularity)}").fetchall()
        gran_ids = [str(row[0]) for row in rows]
        wc = f"granularity_id.value IN {list_to_str(gran_ids)}"
        where_conditions.append(wc)

    # join WHERE conditions together with 'AND' 
    if len(where_conditions) > 0:
        where_cond = " WHERE " + " AND ".join(where_conditions)
    else:
        where_cond = ""

    # form SQLite query
    query = f"""
        SELECT 
            a.id 
        FROM 
            annotation AS a
        LEFT JOIN
            json_each('a'.'tag_id') AS tag_id"""

    if ambiguous:
        query += """
            LEFT JOIN
                json_each('a'.'ambiguous_label_id') AS ambiguous_label_id"""  

    if invert or exclude is not None:
        query += """
            LEFT JOIN
                json_each('a'.'excluded_label_id') AS excluded_label_id"""

    query += """
        LEFT JOIN
            job as j
        ON
            a.job_id = j.id
    """
    query += where_cond
    rows = c.execute(query).fetchall()
    indices = [row[0] for row in rows]
    return indices


def filter_negative(
        conn, 
        source_type=None,
        strict=False,
        taxonomy_id=None,
    ):
    """ Query annotation table by filtering on auto-generated negatives.

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple, list(tuple)                
                Select auto-generated annotations guaranteed to not contain any 
                sounds of the the class (source,type) or descedant classes.
                The character '%' can be used as wildcard.
                Accepts both a single tuple and a list of tuples.
                By default all descendant nodes in the taxonomy tree are also considered. Use 
                the @strict argument to change this behaviour.
            strict: bool
                Whether to interpret labels 'strictly', meaning that descendant nodes 
                in the taxonomy tree are not considered. For example, when filtering on 'KW' 
                annotations labelled as 'SRKW' will *not* be selected if @strict is set to True. 
            taxonomy_id: int
                Acoustic taxonomy that the (source,type) label arguments refer to. If not specified, 
                the latest taxonomy will be used.

        Returns:
            indices: list(int)
                Annotation indices  
    """
    if strict:
        raise NotImplementedError("`strict` not yet implemented")

    c = conn.cursor()

    (auto_neg_id,) = c.execute(f"SELECT id FROM tag WHERE name = '{ktb.AUTO_NEG}'").fetchall()[0]
    where_cond = f" WHERE tag_id.value = {auto_neg_id}"
    if source_type is not None:
        where_cond += " AND "
        where_cond += _primary_sound_condition(conn, source_type, taxonomy_id)

    query = f"""
        SELECT 
            a.id 
        FROM 
            annotation AS a
        LEFT JOIN
            json_each('a'.'tag_id') AS tag_id  
        LEFT JOIN
            job as j
        ON
            a.job_id = j.id
    """
    query += where_cond
    rows = c.execute(query).fetchall()
    indices = [row[0] for row in rows]
    return indices


def _select_label_condition(conn, source_type, strict, tentative, ambiguous, taxonomy_id):
    """ Helper function for @filter_annotation.

        Forms the WHERE condition required to query the annotation table 
        for entries with specific (source,type) labels.

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple, list(tuple)                
                Select annotations with this (source,type) label.
                By default descendant nodes in the taxonomy tree are also considered.
            strict: bool
                Whether to interpret labels 'strictly', meaning that descendant nodes 
                in the taxonomy tree are not considered. For example, when filtering on 'KW' 
                annotations labelled as 'SRKW' will *not* be selected if @strict is set to True. 
            tentative: bool
                Whether to also filter on tentative label assignments. 
            ambiguous: bool
                Whether to also filter on ambiguous label assignments.
            taxonomy_id: int
                Acoustic taxonomy that the (source,type) label arguments refer to. 
            
        Returns:
            wc: str
                WHERE conditions for SQLite query
    """
    if strict:
        raise NotImplementedError("@strict not yet implemented")

    c = conn.cursor()

    # get taxonomy identifiers
    tax_ids = [tax_id for (tax_id,) in c.execute("SELECT id FROM taxonomy").fetchall()]
    if taxonomy_id is None:
        taxonomy_id = len(tax_ids)

    # map source-type tuple to label identifiers
    ids = get_label_id(conn, source_type=source_type, taxonomy_id=taxonomy_id, always_list=True)

    # crosswalk to all taxonomies, including descendants
    ids_crosswalk = dict()
    for tax_id in tax_ids:
        ids_x = klb.crosswalk_label_ids(conn, ids, dst_taxonomy_id=tax_id, ascend=False, descend=True, equiv=True)
        ids_crosswalk[tax_id] = np.unique(ids_x).tolist()

    # callable for selecting label identifiers across taxonomies
    def _select_label_id_fcn(label_id, tax_id):
        return (label_id in ids_crosswalk[tax_id])

    conn.create_function("SELECT_LABEL_ID", 2, _select_label_id_fcn)

    # form WHERE condition for SQLite query
    wc = "("
    wc += "SELECT_LABEL_ID(a.label_id, j.taxonomy_id) = 1"

    if tentative:
        wc += " OR SELECT_LABEL_ID(a.tentative_label_id, j.taxonomy_id) = 1"

    if ambiguous:
        wc += " OR SELECT_LABEL_ID(ambiguous_label_id.value, j.taxonomy_id) = 1"

    wc += ")"

    return wc


def _exclude_label_condition(conn, source_type, strict, tentative, ambiguous, taxonomy_id):
    """ Helper function for @filter_annotation.

        Forms the WHERE condition required to query the annotation table 
        for entries that exclude specific (source,type) labels.

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple, list(tuple)                
                Select annotations with this (source,type) label.
                By default descendant nodes in the taxonomy tree are also considered.
            strict: bool
                Whether to interpret labels 'strictly', meaning that descendant nodes 
                in the taxonomy tree are not considered. For example, when filtering on 'KW' 
                annotations labelled as 'SRKW' will *not* be selected if @strict is set to True. 
            tentative: bool
                Whether to also filter on tentative label assignments. 
            ambiguous: bool
                Whether to also filter on ambiguous label assignments.
            taxonomy_id: int
                Acoustic taxonomy that the (source,type) label arguments refer to. 
            
        Returns:
            wc: str
                WHERE conditions for SQLite query
    """
    if strict:
        raise NotImplementedError("@strict not yet implemented")

    c = conn.cursor()

    # get taxonomy identifiers
    tax_ids = [tax_id for (tax_id,) in c.execute("SELECT id FROM taxonomy").fetchall()]
    if taxonomy_id is None:
        taxonomy_id = len(tax_ids)

    # map source-type tuple to label identifiers
    ids = get_label_id(conn, source_type=source_type, taxonomy_id=taxonomy_id, always_list=True)

    # crosswalk to all taxonomies, including ascendants and descendants
    ids_crosswalk = dict()
    for tax_id in tax_ids:
        ids_x = klb.crosswalk_label_ids(conn, ids, dst_taxonomy_id=tax_id, ascend=True, descend=True, equiv=True)
        ids_crosswalk[tax_id] = np.unique(ids_x).tolist()

    # callable for avoding label identifiers across taxonomies
    def _avoid_label_id_fcn(label_id, tax_id):
        return (label_id is not None and label_id not in ids_crosswalk[tax_id])

    conn.create_function("AVOID_LABEL_ID", 2, _avoid_label_id_fcn)

    # crosswalk to all taxonomies, including only ascendants
    asc_ids_crosswalk = dict()
    for tax_id in tax_ids:
        ids_x = klb.crosswalk_label_ids(conn, ids, dst_taxonomy_id=tax_id, ascend=True, descend=False)
        asc_ids_crosswalk[tax_id] = np.unique(ids_x).tolist()

    # callable for selecting excluded label identifiers across taxonomies
    def _select_excluded_label_id_fcn(label_id, tax_id):
        return (label_id is not None and label_id in asc_ids_crosswalk[tax_id])

    conn.create_function("SELECT_EXCLUDED_LABEL_ID", 2, _select_excluded_label_id_fcn)

    # form WHERE condition for SQLite query
    wc = "(("
    wc += "AVOID_LABEL_ID(a.label_id, j.taxonomy_id) = 1"

    if tentative:
        wc += " AND AVOID_LABEL_ID(a.tentative_label_id, j.taxonomy_id) = 1"

    if ambiguous:
        wc += " AND AVOID_LABEL_ID(ambiguous_label_id.value, j.taxonomy_id) = 1"

    wc += ")"
    wc += " OR SELECT_EXCLUDED_LABEL_ID(excluded_label_id.value, j.taxonomy_id) = 1"
    wc += ")"

    return wc


def _invert_select_label_condition(conn, source_type, strict, tentative, ambiguous, taxonomy_id):
    """ Helper function for @filter_annotation.

        Forms the WHERE condition required to query the annotation table 
        for entries that do *not* have certain (source,type) labels.

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple, list(tuple)
                Select annotations that do *not* have this (source,type) label. 
                By default both ancestral and descendant nodes in the taxonomy tree are also considered.
            strict: bool
                Whether to interpret labels 'strictly', meaning that ancestral and descendant nodes 
                in the taxonomy tree are not considered.
            tentative: bool
                Whether to also filter on tentative label assignments. 
            ambiguous: bool
                Whether to also filter on ambiguous label assignments.
            taxonomy_id: int
                Acoustic taxonomy that the (source,type) label arguments refer to. 
            
        Returns:
            wc: str
                WHERE conditions for SQLite query
    """
    if strict:
        raise NotImplementedError("@strict not yet implemented")

    if ambiguous:
        raise NotImplementedError("@ambiguous not yet implemented for inverted filter")

    c = conn.cursor()

    # get taxonomy identifiers
    tax_ids = [tax_id for (tax_id,) in c.execute("SELECT id FROM taxonomy").fetchall()]
    if taxonomy_id is None:
        taxonomy_id = len(tax_ids)

    # map source-type tuple to label identifiers
    ids = get_label_id(conn, source_type=source_type, taxonomy_id=taxonomy_id, always_list=True)

    # crosswalk to all taxonomies, including ancestors and descendants
    ids_crosswalk = dict()
    for tax_id in tax_ids:
        ids_x = klb.crosswalk_label_ids(conn, ids, dst_taxonomy_id=tax_id, ascend=True, descend=True)
        ids_crosswalk[tax_id] = np.unique(ids_x).tolist()

    # callable for avoiding label identifiers across taxonomies
    def _invert_select_label_id_fcn(label_id, tax_id):
        return (label_id is not None and label_id not in ids_crosswalk[tax_id])

    conn.create_function("INVERT_SELECT_LABEL_ID", 2, _invert_select_label_id_fcn)

    # crosswalk to all taxonomies, including only ascendants
    asc_ids_crosswalk = dict()
    for tax_id in tax_ids:
        ids_x = klb.crosswalk_label_ids(conn, ids, dst_taxonomy_id=tax_id, ascend=True, descend=False)
        asc_ids_crosswalk[tax_id] = np.unique(ids_x).tolist()

    # callable for selecting excluded label identifiers across taxonomies
    def _select_excluded_label_id_fcn(label_id, tax_id):
        return (label_id is not None and label_id in asc_ids_crosswalk[tax_id])

    conn.create_function("SELECT_EXCLUDED_LABEL_ID", 2, _select_excluded_label_id_fcn)

    # form WHERE condition for SQLite query
    wc = "("
    wc += "(INVERT_SELECT_LABEL_ID(a.label_id, j.taxonomy_id) = 1"

    if tentative:
        wc += " OR INVERT_SELECT_LABEL_ID(a.tentative_label_id, j.taxonomy_id) = 1"

    wc += ")"

    # also filter on excluded labels
    wc += " OR SELECT_EXCLUDED_LABEL_ID(excluded_label_id.value, j.taxonomy_id) = 1"

    wc += ")"

    return wc


def _primary_sound_condition(conn, source_type, taxonomy_id):
    """ Helper function for @filter_negative.

        Finds the annotation jobs where @source_type was systematically annotated, 
        i.e., is one of the 'primary sounds' or a descendant node.

        Then forms a simple WHERE condition to filter on those jobs.

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple, list(tuple)
                (source,type) label. 
            taxonomy_id: int
                Acoustic taxonomy that the (source,type) label arguments refer to. 
            
        Returns:
            wc: str
                WHERE conditions for SQLite query
    """
    c = conn.cursor()

    # get taxonomy identifiers
    tax_ids = [tax_id for (tax_id,) in c.execute("SELECT id FROM taxonomy").fetchall()]
    if taxonomy_id is None:
        taxonomy_id = len(tax_ids)

    # load all the taxonomies
    taxonomies = {tax_id: get_taxonomy(conn, tax_id) for tax_id in tax_ids}

    # map source-type tuple to label identifiers
    ids = get_label_id(conn, source_type=source_type, taxonomy_id=taxonomy_id, always_list=True)

    # crosswalk to all taxonomies
    ids_crosswalk = dict()
    for tax_id in tax_ids:
        ids_x = klb.crosswalk_label_ids(conn, ids, dst_taxonomy_id=tax_id)
        ids_crosswalk[tax_id] = np.unique(ids_x).tolist()

    # retrive data for all jobs
    rows = c.execute("SELECT id,taxonomy_id,primary_sound FROM job").fetchall()
    job_ids = []

    # for each job, check if all sounds can be matched to one of the primary sounds
    for job_id, tax_id, prim_sound in rows:
        tax = taxonomies[tax_id]
        sounds = ids_crosswalk[tax_id]
        primary_sounds = json.loads(prim_sound)
        
        num_matches = 0
        for sound in sounds:
            ancestor_sounds = klb.get_related_label_id(conn, sound, ascend=True, descend=False, always_list=True)
            
            for primary_sound in primary_sounds:
                if primary_sound == sound or primary_sound in ancestor_sounds:
                    num_matches += 1
                    break

        if num_matches == len(sounds):
            job_ids.append(job_id)        

    # form WHERE condition for SQLite query
    wc = f"j.id IN {list_to_str(job_ids)}"

    return wc


def get_label_id(conn, source_type=None, taxonomy_id=None, ascend=False, descend=False, always_list=False):
    """ Returns the label identifier corresponding to a sound-source, sound-type tag tuple. 

        If @ascend is set to True, the function will also return the label ids of all the 
        ancestral nodes in the taxonomy tree. For example, if the sound source is specified as 
        SRKW, it will return labels corresponding not only to SRKW, but also KW, Toothed, 
        Cetacean, Mammal, Bio, and Unknown.

        If @descend is set to True, the function will also return the label ids of all the 
        descendant nodes in the taxonomy tree. For example, if the sound source is specified 
        as SRKW, it will return labels corresponding not only to SRKW, but also J, K, and L pod.

        Args:
            conn: sqlite3.Connection
                Database connection
            source_type: tuple(str, str) or list(tuple)
                Sound source and sound type tags. The character '%' can be used as wildcard. 
                For example, use ('SRKW','%') to retrieve all labels associated with the sound 
                source 'SRKW', irrespective of sound type. Multiple source-type pairs can be 
                specified as a list of tuples.
            taxonomy_id: int
                Acoustic taxonomy unique identifier. If not specified, the latest taxonomy will 
                be used.
            ascend: bool
                Also return the labels of ancestral nodes. 
            descend: bool
                Also return the labels of descendant nodes. 
            always_list: bool
                Whether to always return a list. Default is False.

        Returns:
            id: int, list(int)
                Label identifier(s)

        Raises:
            ValueError: if a label with the specified @source_type does not exist                 
    """
    c = conn.cursor()

    tax, taxonomy_id = get_taxonomy(conn, taxonomy_id, return_id=True) #load taxonomy

    if source_type == None:
        source_type = (None, None)

    if isinstance(source_type, tuple):
        source_type = [source_type]

    def _condition(name, val):
        "helper function for forming SQLite conditions"
        if val is None:
            return f"{name} IS NULL"
        else:
            return f"{name} LIKE '{val}'"

    where_condition = f"taxonomy_id = {taxonomy_id} AND ("
    for i,(sound_source, sound_type) in enumerate(source_type):
        # SQLite WHERE conditions
        ss_con = _condition("sound_source_tag", sound_source)
        st_con = _condition("sound_type_tag", sound_type)

        # check that the source,type tuple is valid
        query = f"""
            SELECT 
                id
            FROM 
                label
            WHERE 
                taxonomy_id = {taxonomy_id} AND {ss_con} AND {st_con}
        """
        rows = c.execute(query).fetchall()            
        if len(rows) == 0:
            raise ValueError(f"'{sound_source,sound_type}' does not exist in taxonomy with id {taxonomy_id}")

        if i > 0:
            where_condition += " OR "
        
        where_condition += f"({ss_con} AND {st_con})"

        # reverse search, continue to the root
        if ascend and sound_source not in ["%"]:            
            gen = tax.ascend(sound_source, sound_type, include_start_node=False)
            for i,(s,t) in enumerate(gen): #generator returns (source,type) tag tuple
                ss_con = _condition("sound_source_tag", s)
                st_con = _condition("sound_type_tag", t)
                where_condition += f" OR ({ss_con} AND {st_con})"

        # forward search, continue to the leaves
        if descend and sound_source not in ["%"]:
            gen = tax.descend(sound_source, sound_type, include_start_node=False)
            for i,(s,t) in enumerate(gen): #generator returns (source,type) tag tuple
                ss_con = _condition("sound_source_tag", s)
                st_con = _condition("sound_type_tag", t)
                where_condition += f" OR ({ss_con} AND {st_con})"

    where_condition += ")"

    # SQLite query
    query = f"""
        SELECT 
            id
        FROM 
            label
        WHERE 
            {where_condition}
    """
    rows = c.execute(query).fetchall()
    id = [r[0] for r in rows]

    if not always_list and len(id) == 1:
        id = id[0]

    return id


def assign_files_to_job(conn, job_id, file_id, channel=0, extendable=True):
    """ Associates a set of audio files with an annotation job.

        The association is made by adding (file_id, job_id, channel) 
        tuples to the 'file_job_relation' table in the database.

        Args:
            conn: sqlite3.Connection
                Database connection
            job_id: int
                Annotation job unique identifier
            file_id: int, list(int)
                Audio file unique identifier(s)
            channel: int, list(int), list(list(int))
                For multi-channel recordings, this allows to specify which 
                channels were inspected as part of the annotation job. Can 
                either be a single int, a list of ints with len(channel) = 
                no. channels, or a nested list of ints with len(channel) = no. 
                files and len(channel[i]) = no. channels for file i.
            extendable: bool
                Allow this function to be called multiple times for the same 
                annotation job. True by default. Set to False to help ensure 
                that the database only contains completed annotation jobs.

        Returns:
            counter: int
                Number of entries successfully added to the file_job_relation table.
    """
    c = conn.cursor()

    # make sure that the job does not already have files associated with it
    if not extendable:
        query = f"SELECT EXISTS(SELECT 1 FROM file_job_relation WHERE job_id = {job_id})"
        assert not c.execute(query).fetchone()[0],\
            f"audio files have already been added for job_id={job_id}"

    if isinstance(file_id, int):
        file_id = [file_id]

    if isinstance(channel, int):
        channel = [[channel] for _ in file_id]
    elif isinstance(channel, list) and isinstance(channel[0], int):
        channel = [channel for _ in file_id]

    counter = 0
    for fid,chan_list in zip(file_id, channel):
        for chan in chan_list:
            v = {
                "job_id": job_id,
                "file_id": fid,
                "channel": chan
            }
            try:
                insert_row(conn, table_name="file_job_relation", values=v)
                counter += 1
            
            except sqlite3.IntegrityError:
                logging.error(f"Failed to insert {v} into file_job_relation table. Ignoring entry.")
                continue

    return counter

def add_annotations(conn, annot_tbl, job_id, progress_bar=False, error="replace"):
    """ Add a set of annotations to the database.

        The annotations must be provided in the form of Pandas DataFrame 
        or Python Dictionary with the following structure,

        .. csv-table:: Annotation table
            :file: ./annot.csv
            :widths: 70, 30, 70, 140
            :header-rows: 1
            :delim: ;

        Columns without a default value are mandatory; columns with a 
        default value are optional. 
        
        `deployment_id` and `start_utc` are normally not required, as they are 
        inferred from the `file_id`, but must be specified in cases where the 
        `file_id` column is missing, or some rows have invalid/missing file IDs.

        Annotations without file IDs are inserted into the database with the ID value 0 (zero).

        TODO: chech that tentative (source,type) assignments are more specific cases of confident assignments
        TODO: check that there are no conflicts with existing annotations in the database
                    
        Args:
            conn: sqlite3.Connection
                Database connection
            annot_tbl: pandas DataFrame or dict
                Table of annotations.
            deployment_id: int
                Deployment unique identifier
            job_id: int
                Annotation job unique identifier
            progress_bar: bool
                Display progress bar. Default is False.
            error: str
                Error handling. NOT YET IMPLEMENTED.                
                Options are:

                    * a/abort: If any of the annotations have invalid data, abort the entire submission
                    * i/ignore: Ignore any annotations with invalid data, but proceed with 
                                    submitting all other annotations to the database
                    * r/replace: Automatically replace invalid data fields with default values (where possible) 
                                    and flag the affected annotations for review; 
                                    if replacement is not possible, switch to manual mode.
                    * m/manual: Manually review and fix every annotation with invalid data

        Returns:
            annot_ids: list(int)
                Unique identifiers assigned to the annotations       

        Raises:
            ValueError: If the input table contains annotations with invalid (source,type) assignments.
                        Note: this consistency check is only performed for confident and tentative 
                            assignments, not for ambiguous assignments.
            AssertionError: If the annotation table does not have the required columns.
    """
    ABORT = 0
    IGNORE = 1
    REPLACE = 2
    MANUAL = 3

    if error.lower() in ["a","abort"]:
        error_handling = ABORT
    elif error.lower() in ["i","ignore"]:
        error_handling = IGNORE
    elif error.lower() in ["r","replace"]:
        error_handling = REPLACE
    elif error.lower() in ["m","manual"]:
        error_handling = MANUAL
    else:
        raise ValueError(f"Argument `error` has invalid value: {error}")

    assert "file_id" in annot_tbl.columns or \
        ("deployment_id" in annot_tbl.columns 
            and "start_utc" in annot_tbl.columns \
            and "duration_ms" in annot_tbl.columns), \
        "Annotation table must have column `file_id` or `deployment_id` and `start_utc` and `duration_ms`"

    c = conn.cursor()

    # get information about job
    query = f"SELECT taxonomy_id FROM job WHERE id = '{job_id}'"
    (tax_id,) = c.execute(query).fetchall()[0]

    # if sound_source is not specified, but excluded_sound_source is, set sound_source to Unknown
    if "sound_source" not in annot_tbl.columns and "excluded_sound_source" in annot_tbl.columns:
        annot_tbl["sound_source"] = "Unknown"

    # if sound_type is not specified, just set it to Unknown
    if "sound_type" not in annot_tbl.columns:
        annot_tbl["sound_type"] = "Unknown"

    # ensure required columns have been specified
    required_cols = [
        "file_id", "sound_source", "sound_type"        
    ]
    for col_name in required_cols:
        assert col_name in annot_tbl.columns, f"{col_name} missing from annotation table"

    # enforce integer precision
    int_cols = ["start_ms", "duration_ms"]
    for col in int_cols:
        if col in annot_tbl.columns:
            annot_tbl[col] = annot_tbl[col].round(0).astype(int)

    # default within-file offset
    if "start_ms" not in annot_tbl.columns:
        annot_tbl["start_ms"] = 0

    # default channel
    if "channel" not in annot_tbl.columns:
        annot_tbl["channel"] = 0 

    # default tentative assignments
    if "tentative_sound_source" not in annot_tbl.columns:
        annot_tbl["tentative_sound_source"] = None
    if "tentative_sound_type" not in annot_tbl.columns:
        annot_tbl["tentative_sound_type"] = None

    # default ambiguous assignments
    if "ambiguous_sound_source" not in annot_tbl.columns:
        annot_tbl["ambiguous_sound_source"] = None
    if "ambiguous_sound_type" not in annot_tbl.columns:
        annot_tbl["ambiguous_sound_type"] = None

    # default exclusion assignments
    if "excluded_sound_source" not in annot_tbl.columns:
        annot_tbl["excluded_sound_source"] = None
    if "excluded_sound_type" not in annot_tbl.columns:
        annot_tbl["excluded_sound_type"] = None

    # build file table for this annotation job
    file_tbl = build_file_table(conn, job_id)

    file_tbl.set_index("file_id", inplace=True)

    # obtain file IDs
    def get_filename(row):
        """ Helper function for obtaining filenames.
        
            Attempts to look up the filename in the file table using the file ID, if available.
            If the file is missing/invalid, returns an empty string
        """
        try:
            return file_tbl.loc[row.file_id].filename
        except:
            return ""

    annot_tbl["filename"] = annot_tbl.apply(lambda r: get_filename(r), axis=1) 

    # obtain deployment IDs
    def get_deployment_id(row):
        """ Helper function for obtaining deployment IDs.
        
            Attempts to look up the deployment ID in the file table using the file ID, if available.
            If the file is missing/invalid, returns the value of the deployment_id column.
            If the annotation table does not have a deployment_id column, raises an Error 
        """
        try:
            return file_tbl.loc[row.file_id].deployment_id
        except:
            try:
                return row.deployment_id
            except:
                err_msg = f"Deployment ID required for annotation:\n{row}"
                raise ValueError(err_msg)            

    annot_tbl["deployment_id"] = annot_tbl.apply(lambda r: get_deployment_id(r), axis=1)

    # use filename as index
    file_tbl.reset_index(inplace=True)
    file_tbl.set_index("filename", inplace=True)

    # default duration
    if "duration_ms" not in annot_tbl.columns:
        def _duration_ms_fcn(filename, start_ms):
            return int(file_tbl.loc[filename].num_samples / file_tbl.loc[filename].sample_rate * 1e3 - start_ms)
        
        annot_tbl["duration_ms"] = annot_tbl.apply(lambda r: _duration_ms_fcn(r.filename, r.start_ms), axis=1)

    # helper function for inserting a single annotation into the database
    def insert_annotation(c, file_tbl, row):
        # get file data
        try:
            file_data = file_tbl.loc[row.filename]
            file_id = int(file_data["file_id"])        
            file_start_utc = file_data["start_utc"]
            file_id_list = [int(file_id)]
        
        except:
            file_data = None
            file_id = 0
            file_start_utc = None
            file_id_list = [0]

        # deployment index
        if "deployment_id" in row and file_data is not None:
            assert row.deployment_id == file_data["deployment_id"], "deployment IDs do not match"

        if file_data is not None:
            deployment_id = int(file_data["deployment_id"])

        else:
            deployment_id = row.deployment_id

        # annotation UTC start time
        if file_start_utc is None:
            try:
                start_utc = row.start_utc
            except:
                err_msg = f"UTC start time required for annotation:\n{row}"
                raise ValueError(err_msg)

        else:
            start_utc = file_start_utc + timedelta(microseconds=row.start_ms*1e3)

        # annotation duration
        duration_ms = row.duration_ms
        if "duration_ms" not in row:
            duration_ms = int(file_data["num_samples"] / file_data["sample_rate"] * 1e3)
        else:
            duration_ms = row.duration_ms
        
        # annotation UTC end time
        end_utc = start_utc + timedelta(microseconds=duration_ms*1e3)

        # if the annotation spans multiple files, capture all file ids
        if file_data is not None:
            file_end_utc = file_data["end_utc"]
            filename = row.filename
            while end_utc > file_end_utc:
                loc = file_tbl.index.get_loc(filename) + 1
                if loc >= len(file_tbl):
                    break
                file_data = file_tbl.iloc[loc]
                file_id_list.append(int(file_data["file_id"]))
                file_end_utc = file_data["end_utc"]
                filename = file_tbl.index[loc]

        # (confident) label id
        label_id = get_label_id(
            conn, 
            source_type=(row.sound_source, row.sound_type), 
            taxonomy_id=tax_id,
            always_list=True
        )[0]

        # tentative label id
        if row.tentative_sound_source is None and row.tentative_sound_type is None:
            tent_label_id = None
        else:
            if row.tentative_sound_source is None and row.tentative_sound_type is not None:
                tent_ss = row.sound_source
                tent_st = row.tentative_sound_type
            elif row.tentative_sound_source is not None and row.tentative_sound_type is None:
                tent_ss = row.tentative_sound_source
                tent_st = row.sound_type
            else:
                tent_ss = row.tentative_sound_source
                tent_st = row.tentative_sound_type

            tent_label_id = get_label_id(
                conn, 
                source_type=(tent_ss, tent_st),
                taxonomy_id=tax_id,
                always_list=True
            )[0]

        # ambiguous and excluded label ids
        def _infer_label_ids(s,t,sx,tx):
            """ Helper function for inferring ambiguous/excluded label IDs
            s: sound source (confident)
            t: sound type (confident)
            sx: sound source (ambiguous or excluded)
            tx: sound type (ambiguous or excluded)
            """
            if sx is None and tx is None:
                return [None]
            
            if sx is None:
                sx = [s]
            elif isinstance(sx, str):
                sx = sx.split(",")

            if tx is None:
                tx = [t]
            elif isinstance(tx, str):
                tx = tx.split(",")

            # TODO: also combine with confident assignments

            # form all possible combinations
            combinations = [(src, typ) for typ in tx for src in sx]

            ids = []
            for (src, typ) in combinations:
                try:
                    lid = get_label_id(conn, source_type=(src, typ), taxonomy_id=tax_id)
                    ids.append(lid)
                
                except ValueError:
                    msg = f"Invalid ambiguous/excluded label ({src}, {typ})"

                    if error_handling == IGNORE:
                        msg += " ignored"
                        logging.warning(msg)

                    else:
                        raise ValueError(msg)

            if len(ids) == 0:
                ids = None

            return ids

        # ambiguous labels
        ambi_label_id = _infer_label_ids(
            row.sound_source, 
            row.sound_type, 
            row.ambiguous_sound_source, 
            row.ambiguous_sound_type
        )

        # excluded labels
        excl_label_id = _infer_label_ids(
            row.sound_source, 
            row.sound_type, 
            row.excluded_sound_source, 
            row.excluded_sound_type
        )

        # collect data in a dict        
        v = {
            "job_id": job_id,
            "deployment_id": deployment_id,
            "file_id": file_id,
            "label_id": label_id,
            "tentative_label_id": tent_label_id,
            "ambiguous_label_id": json.dumps(ambi_label_id),  
            "excluded_label_id": json.dumps(excl_label_id),  
            "num_files": len(file_id_list),
            "file_id_list": json.dumps(file_id_list), 
            "start_utc": start_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if isinstance(start_utc, datetime) else start_utc,
            "duration_ms": duration_ms,
            "start_ms": row.start_ms,
            "channel": row.channel,
            "valid": row.valid if "valid" in row else 1,
        }

        # add optional fields
        if "tag" in row:
            tags = row.tag.split(",") if isinstance(row.tag, str) else row.tag
            rows = c.execute(f"SELECT id FROM tag WHERE name IN {list_to_str(tags)}").fetchall()
            tag_ids = [row[0] for row in rows]
        else:
            tag_ids = []

        v["tag_id"] = json.dumps(tag_ids) if len(tag_ids) > 0 else json.dumps(None)

        if "machine_prediction" in row:
            v["machine_prediction"] = json.dumps(row.get("machine_prediction"))

        if "comments" in row:
            v["comments"] = row.comments

        if "freq_min_hz" in row and \
            row.freq_min_hz is not None and \
            not np.isnan(row.freq_min_hz):
            
            v["freq_min_hz"] = int(np.round(row.freq_min_hz))
            
        if "freq_max_hz" in row and \
            row.freq_max_hz is not None and \
            not np.isnan(row.freq_max_hz):

            v["freq_max_hz"] = int(np.round(row.freq_max_hz))

        else:
            if file_data is None:
                v["freq_max_hz"] = np.nan
            else:
                v["freq_max_hz"] = int(file_data["sample_rate"] // 2)

        if "granularity" in row:
            rows = c.execute(f"SELECT id FROM granularity WHERE name = '{row.granularity}'").fetchall()
            if len(rows) == 0:
                raise ValueError(f"Unrecognized annotation granularity: {row.granularity}")

            v["granularity_id"] = rows[0][0]

        # replace invalid frequency ranges with default values and flag annotation for review
        if error_handling == REPLACE and \
            (np.isnan(v["freq_max_hz"]) or v["freq_max_hz"] <= v.get("freq_min_hz", 0)):

            fmin = v["freq_min_hz"]
            fmax = v["freq_max_hz"]
            new_fmin = 0
            new_fmax = 256000 if file_data is None else int(file_data["sample_rate"] // 2)
            v["freq_min_hz"] = new_fmin
            v["freq_max_hz"] = new_fmax
            v["valid"] = 0

            comments = v.get("comments", "")
            if len(comments) > 0:
                comments += "; "
                
            warn_msg = "Invalid frequency range replaced with default range to allow insertion into database:" \
                + f" [{fmin:.0f},{fmax:.0f}] -> [{new_fmin:.0f},{new_fmax:.0f}] (Hz)"
            v["comments"] = comments + warn_msg

            warn_msg += f"; entry flagged for review (valid=0)."
            logging.warning(warn_msg)

        # insert row into database
        c = insert_row(conn, table_name="annotation", values=v)

        return c

    # ensure input is pandas dataframe
    if isinstance(annot_tbl, dict):
        annot_tbl = pd.DataFrame(annot_tbl)

    annot_ids = []

    # loop over all annotations
    for idx,row in tqdm(annot_tbl.iterrows(), disable=not progress_bar, total=annot_tbl.shape[0]):
        while True:

            try:
                c = insert_annotation(c, file_tbl, row)
                annot_ids.append(c.lastrowid)
                break

            except KeyboardInterrupt:
                raise

            except:
                msg = f"Failed to add annotation {idx} to the database."

                if error_handling == ABORT:
                    logging.error(traceback.format_exc())
                    logging.error(msg)
                    raise

                elif error_handling == IGNORE:
                    msg += ". Ignoring entry."
                    logging.debug(traceback.format_exc())
                    logging.warning(msg)
                    break

                elif error_handling in [REPLACE, MANUAL]:
                    logging.error(traceback.format_exc())
                    logging.error(msg)
                    row = edit_row_manually(idx, row)
                    if row is None:
                        logging.info(f"Ignoring entry {idx}")
                        break

    return annot_ids


def add_negatives(conn, job_id):
    """ Auto-generate 'negative' annotations for a specified annotation job.

        Note: This function should only be called once the annotation job is 
        complete and all annotations have been submitted to the database.

        TODO: consider renaming this function to generate_negatives
        TODO: add option to remove existing, auto-generated negatives for this job
                e.g., with a boolen argument @replace with default value True

        Args:
            conn: sqlite3.Connection
                Database connection
            job_id: int
                Annotation job index. Note that the job must be 'exhaustive'

        Returns:
            annot_ids: list(int)
                Unique identifiers assigned to the annotations

        Raises:
            AssertionError: if the job is not 'exhaustive'
    """
    c = conn.cursor()

    # get information about job
    query = f"SELECT is_exhaustive FROM job WHERE id = '{job_id}'"
    (is_exhaustive,) = c.execute(query).fetchall()[0]

    # make sure that annotations are 'exhaustive'
    assert is_exhaustive, "not possible to automatically generate 'negatives' for non-exhaustive annotation jobs"

    # filter annotations based on job ID
    rows = c.execute(f"SELECT id FROM annotation WHERE job_id = {job_id}").fetchall()
    indices = [row[0] for row in rows]

    # get a suitably formatted annotation table
    annot_tbl = get_annotations(conn, indices)

    # build the file table
    file_tbl = build_file_table(conn, job_id)

    # add filename and deployment_id columns to annotation table
    file_tbl.set_index("file_id", inplace=True)
    annot_tbl["filename"] = annot_tbl.file_id.apply(lambda x: file_tbl.loc[x].filename) 
    annot_tbl["deployment_id"] = annot_tbl.file_id.apply(lambda x: file_tbl.loc[x].deployment_id)

    # identify 'absence' periods
    c = conn.cursor()
    (auto_neg_id,) = c.execute(f"SELECT id FROM tag WHERE name = '{ktb.AUTO_NEG}'").fetchall()[0]
    neg_tbl = find_negatives(file_tbl, annot_tbl, tag_id=auto_neg_id)

    # add them to the database tagged as 'NEGATIVE'
    annot_ids = add_annotations(conn, neg_tbl, job_id)
    
    return annot_ids


def get_annotations(conn, indices=None, ketos=False, tentative=False, top=False, ketos_v3=False):
    """ Get annotations specified by index
    
        TODO: add @group_by_deployment arg
        TODO: consider replacing @top arg with @full_path arg
        TODO: Handle cases where len(indices) = 0 more gracefully
        TODO: Remove columns with exclusively 'None' values from the returned DataFrame

        Args:
            conn: sqlite3.Connection
                Database connection
            indices: list(int)
                Indices in the annotation table
            ketos: bool
                Return in Ketos format. Default is False
            tentative: bool
                Whether to use tentative assignments, when available. Only applicable if @ketos=True.
            top: bool
                Whether to include the file location in the output table. Only applicable if @ketos=True.
            ketos_v3: bool
                Use the new Ketos 3.0 format

        Returns:
            annot_tbl: Pandas DataFrame
                Annotation table
            label_map: dict
                Mapping from integer label to (source,type) label. Only returned if @ketos=True
    """
    c = conn.cursor()

    # SQLite query
    where_cond = f"WHERE a.id IN {list_to_str(indices)}" if indices is not None else ""
    query = f"""
        SELECT
            a.job_id,
            a.deployment_id,
            a.file_id,
            l.sound_source_tag,
            l.sound_type_tag,
            lt.sound_source_tag,
            lt.sound_type_tag,
            a.tag_id,  
            a.start_utc,
            a.duration_ms,
            a.start_ms,
            a.freq_min_hz,
            a.freq_max_hz,
            a.channel,
            g.name,
            a.machine_prediction,
            a.comments,
            f.filename,
            f.relative_path,
            s.path,
            a.num_files,
            a.file_id_list,
            a.valid,
            a.ambiguous_label_id
        FROM
            annotation AS a
        LEFT JOIN
            label AS l
        ON
            a.label_id = l.id
        LEFT JOIN
            label AS lt
        ON
            a.tentative_label_id = lt.id
        LEFT JOIN
            file as f
        ON 
            f.id = a.file_id
        LEFT JOIN
            storage as s
        ON 
            s.id = f.storage_id
        LEFT JOIN
            granularity as g
        ON 
            g.id = a.granularity_id
        {where_cond}
    """
    rows = c.execute(query).fetchall()

    # reformat JSON entries
    n_cols = len(rows[0])
    data = [[] for _ in range(n_cols)]
    for row in rows:
        for i,v in enumerate(row):
            if (i == 7 or i == 21 or i == 23) and v is not None:  #tag, file_id_list, ambiguous_label_id
                v = json.loads(v)

            data[i].append(v)

    # collect in a Pandas DataFrame
    annot_tbl = pd.DataFrame(
        {
            "job_id": pd.Series(data[0], dtype="int"),
            "deployment_id": pd.Series(data[1], dtype="int"),
            "file_id": pd.Series(data[2], dtype="int"),
            "sound_source": pd.Series(data[3], dtype="str"),
            "sound_type": pd.Series(data[4], dtype="str"),
            "tentative_sound_source": pd.Series(data[5], dtype="str"),
            "tentative_sound_type": pd.Series(data[6], dtype="str"),
            "tag_id": pd.Series(data[7], dtype="object"),
            "start_utc": pd.Series(data[8], dtype="datetime64[ns]"),
            "duration_ms": pd.Series(data[9], dtype="float").astype("int"),
            "start_ms": pd.Series(data[10], dtype="float").astype("int"),
            "freq_min_hz": pd.Series(data[11], dtype="float").astype("int"),
            "freq_max_hz": pd.Series(data[12], dtype="float").astype("int"),
            "channel": pd.Series(data[13], dtype="int"),
            "granularity": pd.Series(data[14], dtype="str"),
            "machine_prediction": pd.Series(data[15], dtype="object"),
            "comments": pd.Series(data[16], dtype="str"),
            "filename": pd.Series(data[17], dtype="str"),
            "relative_path": pd.Series(data[18], dtype="str"),
            "top_path": pd.Series(data[19], dtype="str"),
            "num_files": pd.Series(data[20], dtype="int"),
            "file_id_list": pd.Series(data[21], dtype="object"),
            "valid": pd.Series(data[22], dtype="int"),
            "ambiguous_label_id": pd.Series(data[23], dtype="object"),
        }
    )

    # convert tag IDs to tag names
    rows = c.execute(f"SELECT id,name FROM tag").fetchall()
    tag_map = {row[0]: row[1] for row in rows}
    idx = annot_tbl.tag_id.isna()   
    annot_tbl["tag"] = None
    annot_tbl.loc[~idx, "tag"] = annot_tbl.loc[~idx].tag_id.apply(lambda x: [tag_map[tag_id] for tag_id in x])

    # convert ambiguous label IDs to source/type labels
    idx = annot_tbl.ambiguous_label_id.isna()   
    annot_tbl["ambiguous_label"] = None
    def lookup_label(label_ids):
        """ Helper function for looking up source/type labels """
        labels = []
        for label_id in label_ids:
            if label_id is None:
                continue

            query = f"SELECT sound_source_tag,sound_type_tag FROM label WHERE id = {label_id}"
            rows = c.execute(query).fetchall()
            src, typ = rows[0]
            labels.append((src,typ))

        if len(labels) == 0:
            return None
        else:
            return labels

    annot_tbl.loc[~idx, "ambiguous_label"] = annot_tbl.loc[~idx].ambiguous_label_id.apply(lambda x: lookup_label(x))

    if ketos:
        annot_tbl = _convert_to_ketos(
            annot_tbl, tentative=tentative, top=top, v3=ketos_v3, conn=conn
        )
        return annot_tbl

    else:
        drop_cols = [
            "filename","relative_path","top_path","num_files","file_id_list","tag_id","ambiguous_label_id"
        ]
        annot_tbl.drop(columns=drop_cols, inplace=True)
        return annot_tbl


def _convert_to_ketos(annot_tbl, tentative=False, top=False, v3=False, conn=None):
    """ Helper function for @get_annotations. Converts annotation table to Ketos-friendly format.
    
        TODO: Allow for tentative/ambiguous labels to be included in Ketos-formatted return table

        Args:
            annot_tbl: Pandas DataFrame
                Annotation table
            tentative: bool
                Whether to use tentative assignments, when available.
            top: bool
                Whether to include the path to the top directory in the output table. 
            v3: bool
                Use the new Ketos 3.0 format
            conn: sqlite3.Connection
                Database connection. Only required if @v3 is True.

        Returns:
            annot_tbl: Pandas DataFrame
                Annotation table in Ketos-friendly format
            label_map: dict
                Mapping from integer label to (source,type) label
    """
    # create 'label' column with int labels 0,1,2,...
    def _label_fcn(r):
        tag = r.tag
        ss = r.sound_source
        st = r.sound_type
        if tentative:
            if r.tentative_sound_source is not None:
                ss = r.tentative_sound_source
            if r.tentative_sound_type is not None:
                st = r.tentative_sound_type
        if ss is None and st is None and tag is not None:
            return ";".join(tag) if isinstance(tag, list) else tag 
        else:
            return ss + ";" + st

    annot_tbl["label"] = annot_tbl.apply(lambda r: _label_fcn(r), axis=1)
    unique_labels = annot_tbl.label.unique()
    label_map = {label: count for count,label in enumerate(unique_labels)}
    annot_tbl.label = annot_tbl.label.apply(lambda x: label_map[x])

    if v3:
        c = conn.cursor()

        # create column to store annotation IDs
        annot_tbl["annot_id"] = annot_tbl.index.values       

        # unfold annotations that span multiple files
        idx_mf = (annot_tbl.num_files > 1)
        df = annot_tbl[idx_mf]
        data = []
        for idx,row in df.iterrows():
            remaining_ms = row.duration_ms
            for j,file_id in enumerate(row.file_id_list):
                query = f"SELECT num_samples,sample_rate FROM file WHERE id = {file_id}"
                (num_samples, sample_rate) = c.execute(query).fetchall()[0]
                file_duration_ms = 1000. * num_samples / sample_rate
                new_row = row.copy()
                if j > 0:
                    new_row.start_ms = 0

                new_row.duration_ms = min(remaining_ms, file_duration_ms - new_row.start_ms)
                new_row.start_utc = row.start_utc + timedelta(microseconds=1E3*(row.duration_ms - remaining_ms))
                data.append(new_row)
                remaining_ms -= new_row.duration_ms

        if len(data) > 0:
            df_unfolded = pd.DataFrame(data, columns=df.columns)

            # re-join with other annotations
            annot_tbl = pd.concat([annot_tbl[~idx_mf], df_unfolded])

        # sort
        annot_tbl.sort_values(by=["annot_id", "start_utc"], inplace=True)


    # convert ms -> s
    annot_tbl["start"] = annot_tbl.start_ms.astype(float) / 1e3
    annot_tbl["duration"] = annot_tbl.duration_ms.astype(float) / 1e3

    # rename
    annot_tbl.rename(columns={"freq_min_hz":"freq_min", "freq_max_hz":"freq_max"}, inplace=True)

    # only keep relevant columns
    cols = ["annot_id"] if v3 else []
    cols += ["filename", "relative_path","start","duration","freq_min","freq_max","label","comments"]
    if top: 
        cols += ["top_path"]
    
    annot_tbl = annot_tbl[cols]

    # use multi-index
    if v3:
        annot_tbl.set_index(["annot_id","filename"], inplace=True)

    # invert label_map 
    label_map = {v:k for k,v in label_map.items()}

    return annot_tbl, label_map


def find_negatives(file_tbl, annot_tbl, max_gap_ms=100, tag_id=1):
    """ Find time periods without annotations, also referred to as 'negatives'.

        Args:
            file_tbl: pandas.DataFrame
                Table of audio files generated by :func:`build_file_table`.
            annot_tbl: pandas.DataFrame
                Table of annotations.
            max_gap_ms: int
                Negatives are allowed to span multiple audio files (from 
                the same deployment) provided the temporal gap between the 
                files is below this value.
            tag_id: int
                Tag index assigned to negatives

        Returns:
            neg_tbl: pandas.DataFrame
                Negatives annotation table 
    """
    # use filename as index
    file_tbl.reset_index(inplace=True)
    file_tbl.set_index("filename", inplace=True)

    # add start/end UTC times to annotation table
    def annot_start_utc_fcn(r):
        return file_tbl.loc[r.filename].start_utc + timedelta(microseconds=r.start_ms*1e3)    
    
    annot_tbl["start_utc"] = annot_tbl.apply(lambda r: annot_start_utc_fcn(r), axis=1)
    annot_tbl["end_utc"] = annot_tbl.apply(lambda r: r.start_utc + timedelta(microseconds=r.duration_ms*1e3), axis=1)

    # sort chronologically
    file_tbl.sort_values(by="start_utc", inplace=True)
    annot_tbl.sort_values(by="start_utc", inplace=True)

    # reindex tables
    file_tbl.reset_index(inplace=True)
    file_tbl.set_index(["deployment_id","start_utc"], inplace=True)
    annot_tbl.reset_index(inplace=True)
    annot_tbl.set_index(["deployment_id","channel","start_utc"], inplace=True)

    neg_tbl = []

    # loop over deployments
    for deploy_id, file_tbl_deploy in file_tbl.groupby(level=0):        
        annot_tbl_deploy = annot_tbl.loc[deploy_id]

        end_utc_prev = dict()
        negative = dict()

        # loop over files
        for (_,file_start_utc), file_row in file_tbl_deploy.iterrows(): 

            # loop over channels
            for chan in [int(c) for c in file_row.channel.split(";")]:

                # if there is an 'open' negative ...
                if chan in negative:
                    # ... check if its start point lies within the previous file or this one,
                    # and update filename and file start time as appropriate
                    if negative[chan]["start_utc"] >= file_start_utc:
                        negative[chan]["filename"] = file_row.filename
                        negative[chan]["file_start_utc"] = file_start_utc

                    # ... and find out if it can continue or needs to be terminated
                    if chan in end_utc_prev:
                        gap_ms = (file_start_utc - end_utc_prev[chan]).total_seconds() * 1000
                        terminate = (gap_ms > max_gap_ms)
                    else:
                        terminate = True

                    if terminate:
                        negative[chan]["end_utc"] = end_utc_prev[chan]
                        neg_tbl.append(negative.pop(chan))                        
                        
                # if there is are no 'open' negatives, start a new one
                if chan not in negative:
                    negative[chan] = {
                        "file_id": file_row.file_id,
                        "deployment_id": deploy_id,
                        "channel": chan,
                        "filename": file_row.filename,
                        "file_start_utc": file_start_utc, 
                        "start_utc": file_start_utc, 
                    }

                # update previous file end UTC time
                end_utc_prev[chan] = file_row.end_utc

                # select annotations for current channel
                try:
                    annot_tbl_chan = annot_tbl_deploy.loc[chan]
                except KeyError:
                    continue

                # select annotations that have start time within current file
                annot_tbl_fil = annot_tbl_chan[(annot_tbl_chan.index >= file_start_utc) & (annot_tbl_chan.index < file_row.end_utc)]

                # loop over selected annotations
                for start_utc, annot_row in annot_tbl_fil.iterrows():
                    if start_utc <= negative[chan]["start_utc"]:
                        negative[chan]["start_utc"] = max(annot_row.end_utc, negative[chan]["start_utc"])
                    else:
                        # terminate 'open' negative
                        negative[chan]["end_utc"] = start_utc
                        neg_tbl.append(negative.pop(chan))
                        # start next one
                        negative[chan] = {
                            "file_id": file_row.file_id,
                            "deployment_id": deploy_id,
                            "channel": chan,
                            "filename": file_row.filename,
                            "file_start_utc": file_start_utc, 
                            "start_utc": annot_row.end_utc, 
                        }

    # terminate any negatives that are still 'open'
    for chan,qp in negative.items():
        negative[chan]["end_utc"] = end_utc_prev[chan]
        neg_tbl.append(negative[chan])

    # prep return table
    neg_tbl = pd.DataFrame(neg_tbl)
    neg_tbl["start_ms"] = neg_tbl.apply(lambda r: (r.start_utc - r.file_start_utc).total_seconds() * 1000, axis=1).astype(int)
    neg_tbl["duration_ms"] = neg_tbl.apply(lambda r: (r.end_utc - r.start_utc).total_seconds() * 1000, axis=1).astype(int)
    neg_tbl.drop(columns=["file_start_utc","start_utc","end_utc"], inplace=True)

    # filter out periods with 0 or negative duration
    neg_tbl = neg_tbl[neg_tbl.duration_ms > 0]

    # add required columns
    neg_tbl["tag"] = ktb.AUTO_NEG
    neg_tbl["sound_source"] = None
    neg_tbl["sound_type"] = None
    neg_tbl["tentative_sound_source"] = None
    neg_tbl["tentative_sound_type"] = None
    neg_tbl["freq_min_hz"] = None
    neg_tbl["freq_max_hz"] = None
    neg_tbl["granularity_id"] = 2  #window
    neg_tbl["comments"] = None

    # return file table to original indexing
    file_tbl.reset_index(inplace=True)
    file_tbl.set_index("filename", inplace=True)

    return neg_tbl


def build_file_table(conn, job_id, top=False):
    """ Returns a table with the audio files that were inspected as part 
        of a given annotation job or set of jobs.

        The table has the following columns,

         * file_id (int): audio file unique identifier
         * deployment_id (int): deployment unique identifier
         * filename (str): audio filename
         * relative_path (str): relative path to audio file 
         * sample_rate (int): sampling rate in samples/s
         * start_utc (datetime): file UTC start time
         * end_utc (datetime): file UTC end time
         * channel (str): the channels that were inspected (0;1;...)

        Optionally, the following columns may be included,

         * top_path (str): path to the top directory, relative to which the 
            audiofile relative paths are specified.

        Args:
            conn: sqlite3.Connection
                Database connection
            job_id: int, list(int)
                Annotation job unique identifier(s)
            top: bool
                Whether to include the path to the top directory

        Returns:
            file_tbl: pandas.DataFrame
                File table
    """
    c = conn.cursor()

    # query file data
    query = f"SELECT file_id,channel FROM file_job_relation WHERE job_id IN {list_to_str(job_id)}"
    rows = c.execute(query).fetchall()

    file_ids = np.unique([r[0] for r in rows])

    channels = pd.DataFrame(rows, columns=["file_id","channel"])\
        .sort_values(by=["file_id","channel"])\
        .astype({"channel":str})\
        .set_index("file_id")

    columns = [
        "f.id",
        "f.deployment_id",
        "f.filename",
        "f.relative_path",
        "f.sample_rate",
        "f.num_samples",
        "f.start_utc",
    ]
    left_join = ""

    if top:
        columns += ["s.path"]
        left_join = "LEFT JOIN storage AS s ON s.id = f.storage_id"

    query = f"""
        SELECT 
            {','.join(columns)} 
        FROM 
            file AS f
        {left_join}
        WHERE 
            f.id IN ({','.join([str(id) for id in file_ids])})
    """
    file_data = c.execute(query).fetchall()

    # pass data to a pandas DataFrame
    columns = [c[c.find(".") + 1 :] for c in columns]
    file_tbl = pd.DataFrame(file_data, columns=columns)

    # rename file id column
    file_tbl.rename(columns={"id":"file_id"}, inplace=True)

    # convert to datetime
    file_tbl.start_utc = pd.to_datetime(file_tbl.start_utc, format="%Y-%m-%d %H:%M:%S.%f")

    # add end_utc column
    file_tbl["end_utc"] = file_tbl.apply(lambda r: r.start_utc + timedelta(microseconds=r.num_samples/r.sample_rate*1e6), axis=1)

    # add channel column
    def join(x):
        if isinstance(x, str):
            return x
        else:
            return ";".join(x.values.tolist())

    file_tbl["channel"] = file_tbl.file_id.apply(lambda x: join(channels.loc[x].channel))

    # sort according to deployment and time, in that order
    file_tbl.sort_values(by=["deployment_id", "start_utc", "channel", "end_utc"], inplace=True)

    return file_tbl


def insert_row(conn, table_name, values):
    """ Insert a row of values into a table in the database.
        
        Args:
            conn: sqlite3.Connection
                Database connection
            table_name: str
                Table name
            values: dict
                Values to be inserted
        
        Returns:    
            c: sqlite3.Cursor
                Database cursor
                
        Raises:
            sqlite3.IntegrityError: if the table already contains an entry with these data
    """
    c = conn.cursor()

    # check if table has its own 'id' column
    has_id = False
    columns = conn.execute(f"PRAGMA table_info({table_name})")
    for col in columns:
        if col[1] == "id":
            has_id = True
            break

    # SQL query
    col_names = ",".join(values.keys())
    val_str = ",".join(["?" for _ in values.keys()])
    if has_id:
        col_names = "id," + col_names
        val_str = "NULL," + val_str
    c.execute(
        f"INSERT INTO {table_name} ({col_names}) VALUES ({val_str})", list(values.values())
    ) 

    return c


def insert_job(conn, values):
    """ Insert an annotation job into the database.
        
        Args:
            conn: sqlite3.Connection
                Database connection
            values: dict
                Values to be inserted
        
        Returns:    
            c: sqlite3.Cursor
                Database cursor
    """
    c = conn.cursor()

    tax_id = values.get("taxonomy_id", None)

    # convert tuples to label identifiers
    if tax_id is not None:
        if "primary_sound" in values:
            try:
                values["primary_sound"] = json.dumps(get_label_id(conn, values["primary_sound"], tax_id, always_list=True))

            except Exception as e:
                v = values["primary_sound"]
                err_msg = f"Attempt to convert primary sound tags {v} to label IDs failed\n" + traceback.format_exc()
                raise RuntimeError(err_msg)

        if "background_sound" in values:
            try:
                values["background_sound"] = json.dumps(get_label_id(conn, values["background_sound"], tax_id, always_list=True))

            except Exception as e:
                v = values["background_sound"]
                err_msg = f"Attempt to convert background sound tags {v} to label IDs failed\n" + traceback.format_exc()
                raise RuntimeError(err_msg)

    # convert bool to int
    if "is_exhaustive" in values:
        values["is_exhaustive"] = int(values["is_exhaustive"])

    return insert_row(conn, "job", values)


def get_taxonomy(conn, taxonomy_id=None, return_id=False):
    """ Loads the specified acoustic taxonomy from the database.

        Args: 
            conn: sqlite3.Connection
                Database connection
            taxonomy_id: int
                Acoustic taxonomy unique identifier. If not specified, 
                the latest added taxonomy will be loaded.
            return_id: bool
                Whether to also return the taxonomy identifier. Default is False.

        Returns:
            tax: kx.AcousticTaxonomy
                Acoustic taxonomy
            id: int
                Taxonomy identifier. Only returned if @return_id has been set to True.
    """
    c = conn.cursor()
    
    if taxonomy_id is None:
        query = f"SELECT id,name,version,tree FROM taxonomy ORDER BY id DESC LIMIT 1"
    else:
        query = f"SELECT id,name,version,tree FROM taxonomy WHERE id = '{taxonomy_id}'"
    
    (id,name,version,data) = c.execute(query).fetchall()[0]
    tax_dict = {"name":name, "version":version, "tree":json.loads(data)}

    rows = c.execute("PRAGMA database_list").fetchall()
    path = rows[0][2]

    tax = kx.AcousticTaxonomy.from_dict(tax_dict, path=path)

    if return_id:
        return tax, id
    else:
        return tax


def insert_taxonomy(conn, tax, comment=None, overwrite=False):
    """ Insert acoustic taxonomy into database.
        
        Also adds all the sound-source, sound-type combinations to the table of allowed labels.

        Args:
            conn: sqlite3.Connection
                Database connection
            tax: kx.AcousticTaxonomy
                Acoustic taxonomy
            comment: str
                Optional field. Typically used for describing the main changes made 
                to the taxonomy since the last version.
            overwrite: bool
                Set to True to allow existing entries in the taxonomy table with the 
                same name and version no. to be overwritten.
        
        Returns:    
            c: sqlite3.Cursor
                Database cursor
                
        Raises:
            sqlite3.IntegrityError: if the database already contains 
                a taxonomy with the same name and version no.
    """
    c = conn.cursor()

    tax_dict = tax.to_dict()

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

    # insert into 'taxonomy' table
    c.execute(
        "INSERT INTO taxonomy VALUES (?, ?, ?, ?, ?, ?)",
        [tax_id, tax_dict["name"], tax_dict["version"], json.dumps(tax_dict["tree"]), timestamp, comment]
    )

    if tax_id is None:
        tax_id = c.lastrowid 

    # add precursors of created nodes
    for created_id, (precursor_id, is_equivalent) in tax.created_nodes.items():
        c.execute("INSERT INTO taxonomy_created_node VALUES (?, ?, ?, ?)", [created_id, json.dumps(precursor_id), int(is_equivalent), tax_id])

    # add inheritors of removed nodes
    for removed_id, (inheritor_id, is_equivalent) in tax.removed_nodes.items():
        c.execute("INSERT INTO taxonomy_removed_node VALUES (?, ?, ?, ?)", [removed_id, json.dumps(inheritor_id), int(is_equivalent), tax_id])

    # add 'empty' label
    if not tax_exists:
        c.execute(
            "INSERT INTO label VALUES (NULL, ?, ?, ?, ?, ?)",
            [tax_id, None, None, None, None]
        ) 

    # add labels for all the source-type pairs from the taxonomy
    for sound_source in tax.all_nodes_itr():
        for sound_type in sound_source.data["sound_types"].all_nodes_itr():
            try:
                c.execute(
                    "INSERT INTO label VALUES (NULL, ?, ?, ?, ?, ?)",
                    [tax_id, sound_source.tag, sound_source.identifier, sound_type.tag, sound_type.identifier]
                ) 
            except sqlite3.IntegrityError as err_msg:
                if not "UNIQUE constraint failed" in str(err_msg):
                    raise sqlite3.IntegrityError(str(err_msg))

    # return db cursor
    return c


def import_taxonomy(conn, src, name, new_name=None):
    """ Import an acoustic taxonomy.

        Args:
            conn: sqlite3.Connection
                Database connection. (The database into which the taxonomy will be imported.)
            src: str
                Path to the database file (.sqlite) from which the taxonomy is being imported.
            name: str
                Name of the taxonomy.
            new_name: str
                Optional field for renaming the taxonomy.
             
        Returns:    
            c: sqlite3.Cursor
                Database cursor
                
        Raises:
            sqlite3.IntegrityError: if the database already contains 
                a taxonomy with the same name.
    """
    c = conn.cursor()

    # attach source database
    c.execute(f"ATTACH DATABASE '{src}' AS src")

    # copy relevant rows from:
    #  - taxonomy    
    query = f"""
        INSERT INTO
            main.taxonomy
        SELECT * FROM
            src.taxonomy AS t
        WHERE
            t.name = '{name}'
    """
    c.execute(query)

    # rename (optional)
    if new_name is not None:
        query = f"""
            UPDATE
                main.taxonomy
            SET
                name = REPLACE(name,'{name}','{new_name}')
        """
        c.execute(query)

    # copy relevant rows from:
    #  - label
    #  - taxonomy_created_node
    #  - taxonomy_removed_node
    for tbl_name in ["label", "taxonomy_created_node", "taxonomy_removed_node"]:
        columns = conn.execute(f"PRAGMA table_info({tbl_name})")
        col_names_str = ",".join(["tbl."+column[1] for column in columns])
        query = f"""
            INSERT INTO
                main.{tbl_name}
            SELECT {col_names_str} FROM
                src.{tbl_name} AS tbl
            LEFT JOIN
                src.taxonomy AS t
            ON
                t.id = tbl.taxonomy_id
            WHERE
                t.name = '{name}'
        """
        c.execute(query)

    return c


def create_db(path):
    """ Create an SQLite database with the Korus schema

    Args:
        path: str
            Full path to the database file (.sqlite)   

    Returns:
        conn: sqlite3.Connection
            Database connection
    """
    if os.path.exists(path):
        ans = input(f"{path} already exists, overwrite? [y/N]")
        if ans.lower() == "y":
            os.remove(path)
        else:
            return

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = on")
    c = conn.cursor()

    # annotation
    ktb.create_annotation_table(conn)

    # job
    ktb.create_job_table(conn)

    # file
    ktb.create_file_table(conn)

    # deployment
    ktb.create_deployment_table(conn)

    # storage
    ktb.create_storage_table(conn)

    # taxonomy
    ktb.create_taxonomy_table(conn)
    ktb.create_taxonomy_created_node_table(conn)
    ktb.create_taxonomy_removed_node_table(conn)

    # model
    ktb.create_model_table(conn)

    # label
    ktb.create_label_table(conn)

    # tag
    ktb.create_tag_table(conn)

    # granularity
    ktb.create_granularity_table(conn)

    # file-job relation
    ktb.create_file_job_relation_table(conn)

    conn.commit()
    return conn
