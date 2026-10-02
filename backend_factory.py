"""Connect workers to the configured database; imports PostgreSQL only when used."""
def connect_backend(cfg, target, read_only=False):
    name = cfg.get('database', 'sqlite')
    if name == 'sqlite':
        from db_backends.sqlite_backend import SQLiteBackend
        backend = SQLiteBackend(target, 'WAL', 'NORMAL')
    elif name == 'postgresql':
        from db_backends.postgres_backend import PostgresBackend
        backend = PostgresBackend(target)
    elif name == 'influxdb':
        from db_backends.influx_backend import InfluxBackend
        backend = InfluxBackend(target, read_only=read_only)
    else:
        raise ValueError(f'Unsupported database: {name}')
    try:
        backend.connect()
        if name == 'sqlite':
            backend.conn.execute('PRAGMA query_only=ON' if read_only
                                 else 'PRAGMA busy_timeout=60000')
        elif name == 'postgresql' and read_only:
            backend.conn.execute('SET default_transaction_read_only=on')
        return backend
    except Exception:
        backend.close()
        raise
