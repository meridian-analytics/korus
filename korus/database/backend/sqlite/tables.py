from .query import table_exists


def create_tables(conn):
    """Create all tables"""
    create_annotation_table(conn)
    create_job_table(conn)
    create_file_table(conn)
    create_deployment_table(conn)
    create_storage_table(conn)
    create_taxonomy_table(conn)
    create_label_table(conn)
    create_tag_table(conn)
    create_granularity_table(conn)
    create_file_job_relation_table(conn)


def is_field_table(table_name):
    return len(table_name) > 5 and table_name[0] == "_" and table_name[-6:] == "_field"


def field_table_name(parent_table_name: str):
    return "_" + parent_table_name + "_field"


def create_field_table(conn, parent_table_name):
    """Create table for storing custom fields.

    The table is named `_{parent_table_name}_field`

    Args:
        conn: sqlite3.Connection
            Database connection
        parent_table_name: str
            Name of the `parent` table
    """
    tbl_name = field_table_name(parent_table_name)
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()

    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                description TEXT NOT NULL,
                required INTEGER DEFAULT 1,
                default_value TEXT,
                options JSON,
                is_path INTEGER DEFAULT 1,
                PRIMARY KEY (id)
            )
        """
    c.execute(tbl_def)


def create_annotation_table(conn):
    """Create annotation table according to Korus schema.

    TODO: Change tentative_label_id type from INTEGER to JSON ? (to allow for lists)
            Or add another column named label_list_id (or similar)

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "annotation"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()

    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                job_id INTEGER NOT NULL,
                deployment_id INTEGER NOT NULL,
                file_id INTEGER,
                label_id INTEGER,
                tentative_label_id INTEGER,
                ambiguous_label_id JSON,
                excluded_label_id JSON,
                multiple_label_id JSON,
                tag_id JSON, 
                granularity_id INTEGER NOT NULL DEFAULT 2,
                negative INTEGER NOT NULL DEFAULT 0,
                num_files INTEGER NOT NULL DEFAULT 1,
                file_id_list JSON,
                start_utc TEXT,
                duration_ms INTEGER,
                start_ms INTEGER DEFAULT 0,
                freq_min_hz INTEGER DEFAULT 0,
                freq_max_hz INTEGER,
                channel INTEGER NOT NULL DEFAULT 0,
                valid INTEGER NOT NULL DEFAULT 1,
                comments TEXT,
                PRIMARY KEY (id),
                FOREIGN KEY (label_id) REFERENCES label (id),
                FOREIGN KEY (tentative_label_id) REFERENCES label (id),
                FOREIGN KEY (job_id) REFERENCES job (id),
                FOREIGN KEY (file_id) REFERENCES file (id),
                FOREIGN KEY (deployment_id) REFERENCES deployment (id),
                FOREIGN KEY (granularity_id) REFERENCES granularity (id),
                CHECK (
                    duration_ms > 0
                    AND freq_min_hz < freq_max_hz
                )
            )
        """
    c.execute(tbl_def)

    create_field_table(conn, tbl_name)


def create_granularity_table(conn):
    """Create granularity table according to Korus schema.

    Also adds entries for the standard Korus granularities: unit, window, file, batch, encounter

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "granularity"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()
    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                PRIMARY KEY (id),
                UNIQUE (name)
            )
        """
    c.execute(tbl_def)

    rows = [
        (
            "unit",
            "Annotation of a single vocalisation/sound. Bounding box drawn snuggly around a single vocalisation/sound."
            " Overlapping sounds may be present.",
        ),
        (
            "window",
            "Annotation of a single vocalisation/sound. Box width does not necessarily match sound duration."
            " Sound may be fully or only partially within the box. Overlapping sounds may be present.",
        ),
        ("file", "Annotation spanning precisely the duration of a single audio file."),
        ("batch", "Annotation of multiple vocalisations/sounds."),
        ("encounter", "Annotation of an entire (biological) acoustic encounter."),
    ]

    for row in rows:
        name = row[0]
        descr = row[1]
        c.execute(
            f"INSERT INTO granularity (id,name,description) VALUES (NULL,?,?)",
            [name, descr],
        )

    create_field_table(conn, tbl_name)


def create_job_table(conn):
    """Create job table according to Korus schema.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "job"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()

    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                taxonomy_id INTEGER,
                annotator TEXT,
                target JSON,
                is_exhaustive INTEGER,
                completion_date TEXT,
                PRIMARY KEY (id),
                FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id),
                CHECK (
                    is_exhaustive IN (0, 1)
                )
            )
        """
    c.execute(tbl_def)

    create_field_table(conn, tbl_name)


def create_deployment_table(conn):
    """Create deployment table according to Korus schema.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "deployment"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()

    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                start_utc TEXT,
                end_utc TEXT,
                latitude_deg REAL,
                longitude_deg REAL,
                depth_m REAL,
                trajectory JSON,
                PRIMARY KEY (id),
                UNIQUE (name, start_utc, end_utc, trajectory)
            )
        """
    c.execute(tbl_def)

    create_field_table(conn, tbl_name)


def create_file_table(conn):
    """Create file table according to Korus schema.

    Also creates an index on (deployment_id, filename) for faster querying.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "file"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()

    # create table
    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                deployment_id INTEGER NOT NULL,
                storage_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                relative_path TEXT NOT NULL DEFAULT '',
                sample_rate INTEGER NOT NULL,
                num_samples INTEGER NOT NULL,
                start_utc TEXT,
                end_utc TEXT,
                PRIMARY KEY (id),
                FOREIGN KEY (deployment_id) REFERENCES deployment (id),
                FOREIGN KEY (storage_id) REFERENCES storage (id),
                UNIQUE (deployment_id, filename, relative_path)
            )
        """
    c.execute(tbl_def)

    # create indices for faster queries
    c.execute(
        """
        CREATE INDEX
            deployment_filename_index
        ON
            file(deployment_id, filename)
    """
    )

    c.execute(
        """
        CREATE INDEX
            deployment_time_index
        ON
            file(deployment_id, start_utc)
    """
    )

    create_field_table(conn, tbl_name)


def create_file_job_relation_table(conn):
    """Create file-job relation table according to Korus schema.

    Also creates an index on (job_id) for faster querying.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    if table_exists(conn, "file_job_relation"):
        return

    c = conn.cursor()

    # create table
    tbl_def = """
        CREATE TABLE
            file_job_relation(
                id INTEGER NOT NULL,
                job_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                channel INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                FOREIGN KEY (job_id) REFERENCES job (id),
                FOREIGN KEY (file_id) REFERENCES file (id),
                UNIQUE (job_id, file_id, channel)
            )
        """
    c.execute(tbl_def)

    # create index for faster queries
    c.execute(
        """
        CREATE INDEX
            job_index
        ON
            file_job_relation(job_id)
    """
    )


def create_storage_table(conn):
    """Create data-storage table according to Korus schema.

    @address can be an IP address or a URL

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "storage"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()
    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                path TEXT NOT NULL DEFAULT '/',
                by_date INTEGER DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE (name, path)
            )
        """
    c.execute(tbl_def)

    create_field_table(conn, tbl_name)


def create_tag_table(conn):
    """Create tag table according to Korus schema.

    Also adds an entry for auto-generated negatives.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "tag"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()
    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                PRIMARY KEY (id),
                UNIQUE (name)
            )
        """
    c.execute(tbl_def)

    create_field_table(conn, tbl_name)


def create_taxonomy_table(conn):
    """Create taxonomy table according to Korus schema.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "taxonomy"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()
    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                version INTEGER,
                tree JSON NOT NULL,
                timestamp TEXT,
                comment TEXT,
                changes JSON,
                created_nodes JSON,
                removed_nodes JSON,
                PRIMARY KEY (id),
                UNIQUE (name, version)
            )
        """
    c.execute(tbl_def)

    create_field_table(conn, tbl_name)


def create_label_table(conn):
    """Create label table according to Korus schema.

    Also creates an index on (taxonomy_id, sound_source_tag, sound_type_tag) for faster querying.

    Args:
        conn: sqlite3.Connection
            Database connection
    """
    tbl_name = "label"
    if table_exists(conn, tbl_name):
        return

    c = conn.cursor()

    # create table
    tbl_def = f"""
        CREATE TABLE
            {tbl_name}(
                id INTEGER NOT NULL,
                taxonomy_id INTEGER NOT NULL,
                sound_source_tag TEXT,
                sound_source_id TEXT,
                sound_type_tag TEXT,
                sound_type_id TEXT,
                PRIMARY KEY (id),
                FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id),
                UNIQUE (taxonomy_id, sound_source_id, sound_type_id)
            )
        """
    c.execute(tbl_def)

    # create index for faster queries
    c.execute(
        """
        CREATE INDEX
            source_type_index
        ON
            label(taxonomy_id, sound_source_tag, sound_type_tag)
    """
    )

    create_field_table(conn, tbl_name)
