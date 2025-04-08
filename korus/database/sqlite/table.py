
def create_file_table(conn):

    c = conn.cursor()

    # check if table already exists

    # table definition
    definition = """
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
    c.execute(definition)

    # indices for faster queries
    indices = [
        """
        CREATE INDEX
            deployment_filename_index
        ON
            file(deployment_id, filename)
        """,
        """
        CREATE INDEX
            deployment_time_index
        ON
            file(deployment_id, start_utc, end_utc)
        """,
    ]
    for index in indices:
        c.execute(index)
