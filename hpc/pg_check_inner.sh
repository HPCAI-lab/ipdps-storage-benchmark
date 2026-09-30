#!/bin/bash
set -euo pipefail
umask 077

tier="${1:?storage tier required}"
case "$tier" in lustre|tmpfs) ;; *) exit 2 ;; esac
[[ "$(hostname)" == nid* ]] || { echo "Compute node required"; exit 2; }
[[ "${SHIFTER_IMAGE:-}" == e4842c8a99ca99339e1693e6fe5fe62c7becb31991f066f989047dfb2fbf47af ]]

actual_fs="$(findmnt -n -r -o FSTYPE -T /pgwork | sort -u)"
[[ "$actual_fs" == "$tier" ]] || {
    echo "Unexpected filesystem: $actual_fs"
    exit 2
}
printf 'STORAGE=%s FILESYSTEM=%s IMAGE=%s\n' "$tier" "$actual_fs" "$SHIFTER_IMAGE"
postgres --version

stop_server() {
    status=$?
    trap - EXIT
    if [[ -f /pgwork/data/postmaster.pid ]]; then
        pg_ctl -D /pgwork/data -m fast -w -t 60 stop || status=1
    fi
    exit "$status"
}
trap stop_server EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

initdb -D /pgwork/data --username="$(id -un)" \
    --auth-local=peer --auth-host=reject --locale=C --encoding=UTF8

pg_ctl -D /pgwork/data -l "/pgresults/server-$tier.log" \
    -o "-c listen_addresses= -c unix_socket_directories=/pgsocket -c unix_socket_permissions=0700 -c fsync=on -c synchronous_commit=on -c full_page_writes=on" \
    -w -t 60 start

psql -X -v ON_ERROR_STOP=1 \
    -h /pgsocket -p 5432 -U "$(id -un)" -d postgres <<'SQL'
SELECT version();
SELECT name, setting FROM pg_settings
WHERE name IN ('data_directory', 'fsync', 'synchronous_commit',
               'full_page_writes', 'shared_buffers', 'max_connections',
               'listen_addresses', 'unix_socket_directories')
ORDER BY name;

CREATE TABLE lifecycle_check (
    id bigint PRIMARY KEY,
    value bigint NOT NULL
);
INSERT INTO lifecycle_check
SELECT i, i * 2 FROM generate_series(1,1000) AS i;

DO $$
BEGIN
    IF (SELECT count(*) FROM lifecycle_check) <> 1000
       OR (SELECT sum(value) FROM lifecycle_check) <> 1001000
       OR (SELECT count(*) FROM lifecycle_check
           WHERE id >= 200 AND id < 300) <> 100
    THEN
        RAISE EXCEPTION 'PostgreSQL data validation failed';
    END IF;
END;
$$;

CHECKPOINT;
SQL

pg_ctl -D /pgwork/data -m fast -w -t 60 stop
[[ ! -f /pgwork/data/postmaster.pid ]]
printf 'PASS: PostgreSQL initialize/write/read/checkpoint/stop on %s\n' "$tier"
