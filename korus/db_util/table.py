def create_annotation_table(conn):
    """ Create annotation table according to Korus schema.

        TODO: Change tentative_label_id type from INTEGER to JSON ? (to allow for lists)
                Or add another column named label_list_id (or similar)
    
        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()

    tbl_def = """
        CREATE TABLE
            annotation(
                id INTEGER NOT NULL,
                job_id INTEGER NOT NULL,
                deployment_id INTEGER NOT NULL,
                file_id INTEGER,
                label_id INTEGER,
                tentative_label_id INTEGER,  
                ambiguous_label_id JSON,
                tag_id JSON, 
                granularity_id INTEGER NOT NULL DEFAULT 2,
                num_files INTEGER NOT NULL DEFAULT 1,
                file_id_list JSON NOT NULL,
                start_utc TEXT,
                duration_ms INTEGER,
                start_ms INTEGER DEFAULT 0,
                freq_min_hz INTEGER DEFAULT 0,
                freq_max_hz INTEGER,
                channel INTEGER NOT NULL DEFAULT 0,
                machine_prediction JSON,
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


def create_granularity_table(conn):
    """ Create granularity table according to Korus schema.

        Also adds entries for the standard Korus granularities: unit, window, file, batch, encounter

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            granularity(
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
            "Annotation of a single vocalisation/sound. Bounding box drawn snuggly around a single vocalisation/sound."\
            " Overlapping sounds may be present."
        ),
        (
            "window",
            "Annotation of a single vocalisation/sound. Box width does not necessarily match sound duration."\
            " Sound may be fully or only partially within the box. Overlapping sounds may be present."
        ),
        (
            "file",
            "Annotation spanning precisely the duration of a single audio file."
        ),
        (
            "batch",
            "Annotation of multiple vocalisations/sounds."
        ),
        (
            "encounter",
            "Annotation of an entire (biological) acoustic encounter."
        ),
    ]

    for row in rows:
        name = row[0]
        descr = row[1]
        c.execute(
            f"INSERT INTO granularity (id,name,description) VALUES (NULL,?,?)", [name,descr]
        )


def create_job_table(conn):
    """ Create job table according to Korus schema.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()

    tbl_def = """
        CREATE TABLE
            job(
                id INTEGER NOT NULL,
                taxonomy_id INTEGER,
                model_id INTEGER,
                annotator TEXT,
                primary_sound JSON,
                background_sound JSON,
                is_exhaustive INTEGER,
                configuration JSON,
                start_utc TEXT,
                end_utc TEXT,
                by_human INTEGER NOT NULL DEFAULT 1,
                by_machine INTEGER NOT NULL DEFAULT 0,
                issues JSON,
                comments TEXT,
                PRIMARY KEY (id),
                FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id),
                FOREIGN KEY (model_id) REFERENCES model (id),
                CHECK (
                    is_exhaustive IN (0, 1)
                    AND by_human IN (0, 1)
                    AND by_machine IN (0, 1)
                    AND (by_human > 0 OR by_machine > 0)
                    AND start_utc < end_utc
                )
            )
        """
    c.execute(tbl_def)


def create_deployment_table(conn):
    """ Create deployment table according to Korus schema.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()

    tbl_def = """
        CREATE TABLE
            deployment(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                owner TEXT,
                start_utc TEXT,
                end_utc TEXT,
                location TEXT,
                latitude_deg REAL,
                longitude_deg REAL,
                depth_m REAL,
                trajectory JSON,
                latitude_min_deg REAL,
                latitude_max_deg REAL,
                longitude_min_deg REAL,
                longitude_max_deg REAL,
                depth_min_m REAL,
                depth_max_m REAL,
                license TEXT,
                hydrophone TEXT,
                bits_per_sample INTEGER,
                sample_rate INTEGER,
                num_channels INTEGER,
                sensitivity REAL,
                comments TEXT,
                PRIMARY KEY (id),
                UNIQUE (owner, name, start_utc, end_utc),
                CHECK (
                    latitude_deg BETWEEN -90 AND 90
                    AND longitude_deg BETWEEN -180 AND 180
                    AND depth_m BETWEEN 0 and 11000
                    AND start_utc <= end_utc 
                )
            )
        """
    c.execute(tbl_def)


def create_file_table(conn):
    """ Create file table according to Korus schema.

        Also creates an index on (deployment_id, filename) for faster querying.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()

    # create table
    tbl_def = """
        CREATE TABLE
            file(
                id INTEGER NOT NULL,
                deployment_id INTEGER NOT NULL,
                storage_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                sample_rate INTEGER NOT NULL,
                num_samples INTEGER NOT NULL,
                downsample TEXT,
                format TEXT,
                codec TEXT,
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
    c.execute("""
        CREATE INDEX
            deployment_filename_index
        ON
            file(deployment_id, filename)
    """
    )

    c.execute("""
        CREATE INDEX
            deployment_time_index
        ON
            file(deployment_id, start_utc, end_utc)
    """
    )


def create_file_job_relation_table(conn):
    """ Create file-job relation table according to Korus schema.

        Also creates an index on (job_id) for faster querying.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()

    # create table
    tbl_def = """
        CREATE TABLE
            file_job_relation(
                job_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                channel INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (job_id) REFERENCES job (id),
                FOREIGN KEY (file_id) REFERENCES file (id),
                UNIQUE (job_id, file_id, channel)
            )
        """
    c.execute(tbl_def)

    # create index for faster queries
    c.execute("""
        CREATE INDEX
            job_index
        ON
            file_job_relation(job_id)
    """
    )


def create_model_table(conn):
    """ Create model table according to Korus schema.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            model(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                version TEXT,
                data JSON NOT NULL,
                PRIMARY KEY (id),
                UNIQUE (name, version)
            )
        """
    c.execute(tbl_def)


def create_storage_table(conn):
    """ Create data-storage table according to Korus schema.

        @address can be an IP address or a URL

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            storage(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                path TEXT NOT NULL DEFAULT '/',
                address TEXT,
                description TEXT,
                PRIMARY KEY (id),
                UNIQUE (name, path, address)
            )
        """
    c.execute(tbl_def)


AUTO_NEG = "__AUTO_NEGATIVE__"

def create_tag_table(conn):
    """ Create tag table according to Korus schema.

        Also adds an entry for auto-generated negatives.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            tag(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                PRIMARY KEY (id),
                UNIQUE (name)
            )
        """
    c.execute(tbl_def)

    # add entry for AUTO_NEG
    descr = "Negative sample, automatically generated by the korus.db.add_negatives function"
    c.execute(
        f"INSERT INTO tag (id,name,description) VALUES (NULL,?,?)", [AUTO_NEG,descr]
    )


def create_taxonomy_table(conn):
    """ Create taxonomy table according to Korus schema.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            taxonomy(
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                version INTEGER,
                tree JSON NOT NULL,
                timestamp TEXT,
                comment TEXT,
                PRIMARY KEY (id),
                UNIQUE (name, version)
            )
        """
    c.execute(tbl_def)


def create_taxonomy_created_node_table(conn):
    """ Create taxonomy_created_node table according to Korus schema.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            taxonomy_created_node(
                id TEXT NOT NULL,
                precursor_id JSON NOT NULL,
                is_equivalent INTEGER NOT NULL,
                taxonomy_id INTEGER NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id)
            )
        """
    c.execute(tbl_def)


def create_taxonomy_removed_node_table(conn):
    """ Create taxonomy_removed_node table according to Korus schema.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()
    tbl_def = """
        CREATE TABLE
            taxonomy_removed_node(
                id TEXT NOT NULL,
                inheritor_id JSON NOT NULL,
                is_equivalent INTEGER NOT NULL,
                taxonomy_id INTEGER NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY (taxonomy_id) REFERENCES taxonomy (id)
            )
        """
    c.execute(tbl_def)


def create_label_table(conn):
    """ Create label table according to Korus schema.

        Also creates an index on (taxonomy_id, sound_source_tag, sound_type_tag) for faster querying.

        Args:
            conn: sqlite3.Connection
                Database connection
    """
    c = conn.cursor()

    # create table
    tbl_def = """
        CREATE TABLE
            label(
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
    c.execute("""
        CREATE INDEX
            source_type_index
        ON
            label(taxonomy_id, sound_source_tag, sound_type_tag)
    """
    )
        
    #c.execute("INSERT INTO label VALUES (NULL, NULL, NULL)")
