"""PostgreSQL adapter for the synthetic metadata and telemetry pilot."""
import json
import psycopg


class PostgresBackend:
    def __init__(self, connection):
        self.connection = connection
        self.conn = None

    def connect(self):
        self.conn = psycopg.connect(**self.connection, autocommit=True)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def get_backend_name(self):
        return "postgresql"

    def initialize_schema(self):
        self.conn.execute("""CREATE TABLE telemetry (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            ts timestamptz NOT NULL, metric text NOT NULL,
            value double precision NOT NULL, tags text)""")
        self.conn.execute("CREATE INDEX idx_telemetry_ts ON telemetry(ts)")
        self.conn.execute("""CREATE TABLE scientific_metadata (
            record_id bigint PRIMARY KEY, timestep bigint NOT NULL,
            variable text NOT NULL, location text NOT NULL,
            value double precision NOT NULL)""")
        self.conn.execute("""CREATE INDEX idx_scientific_metadata_lookup
            ON scientific_metadata(variable, timestep)""")

    def _insert(self, statement, rows):
        with self.conn.transaction():
            with self.conn.cursor() as cursor:
                cursor.executemany(statement, rows)

    def insert_telemetry_batch(self, records):
        rows = [(r['ts'], r['metric'], r['value'], json.dumps(r.get('tags', {})))
                for r in records]
        self._insert("INSERT INTO telemetry(ts, metric, value, tags) VALUES (%s,%s,%s,%s)", rows)

    def insert_scientific_metadata_batch(self, records):
        self._insert("""INSERT INTO scientific_metadata
            (record_id,timestep,variable,location,value) VALUES (%s,%s,%s,%s,%s)""", records)

    def query_telemetry_range(self, start, end):
        return self.conn.execute("""SELECT ts,metric,value,tags FROM telemetry
            WHERE ts >= %s AND ts <= %s ORDER BY ts""", (start, end)).fetchall()

    def query_scientific_metadata_range(self, first, last):
        return self.conn.execute("""SELECT record_id,timestep,variable,location,value
            FROM scientific_metadata WHERE record_id >= %s AND record_id < %s
            ORDER BY record_id""", (first, last)).fetchall()
