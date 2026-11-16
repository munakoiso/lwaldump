#!/bin/sh
set -eu

BIN="/usr/lib/postgresql/${PG_MAJOR}/bin"
ROOT="/tmp/lwaldump-timeline-test"
PRIMARY="$ROOT/primary"
CANDIDATE="$ROOT/candidate"
VOTER="$ROOT/voter"
BRANCH3="$ROOT/branch3"
BRANCH4="$ROOT/branch4"
WAL_ARCHIVE="$ROOT/wal-archive"
PRIMARY_SOCKET="$ROOT/primary-socket"
CANDIDATE_SOCKET="$ROOT/candidate-socket"
VOTER_SOCKET="$ROOT/voter-socket"
BRANCH3_SOCKET="$ROOT/branch3-socket"
BRANCH4_SOCKET="$ROOT/branch4-socket"

mkdir -p "$PRIMARY" "$CANDIDATE" "$VOTER" "$BRANCH3" "$BRANCH4" \
    "$WAL_ARCHIVE" "$PRIMARY_SOCKET" "$CANDIDATE_SOCKET" "$VOTER_SOCKET" \
    "$BRANCH3_SOCKET" "$BRANCH4_SOCKET"
chown -R postgres:postgres "$ROOT"

run_as_postgres() {
    su postgres -c "$*"
}

stop_clusters() {
    run_as_postgres "$BIN/pg_ctl -D '$BRANCH4' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$BRANCH3' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$VOTER' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -m immediate stop" >/dev/null 2>&1 || true
}
trap stop_clusters EXIT

run_as_postgres "$BIN/initdb -D '$PRIMARY' --auth=trust --no-sync" >/dev/null
cat >>"$PRIMARY/postgresql.conf" <<EOF
port = 55442
listen_addresses = ''
unix_socket_directories = '$PRIMARY_SOCKET'
wal_level = replica
max_wal_senders = 5
wal_keep_size = '1GB'
EOF
chown postgres:postgres "$PRIMARY/postgresql.conf"

run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -l '$ROOT/primary.log' -w start" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION lwaldump'" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)'" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$PRIMARY_SOCKET' -p 55442 -D '$CANDIDATE' -X stream -R" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$PRIMARY_SOCKET' -p 55442 -D '$VOTER' -X stream -R" >/dev/null
chmod 700 "$CANDIDATE" "$VOTER"

cat >>"$CANDIDATE/postgresql.conf" <<EOF
port = 55443
listen_addresses = ''
unix_socket_directories = '$CANDIDATE_SOCKET'
hot_standby = on
wal_keep_size = '1GB'
restore_command = ''
EOF
chown postgres:postgres "$CANDIDATE/postgresql.conf"
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -l '$ROOT/candidate.log' -w start" >/dev/null

run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(1, 50000) AS g\"" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c 'SELECT pg_switch_wal()'" >/dev/null
fork_lsn="$(run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"
fork_offset="$(run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -Atc \"SELECT pg_wal_lsn_diff('$fork_lsn', '0/0')::numeric % pg_size_bytes(current_setting('wal_segment_size'))\"")"
[ "$fork_offset" = "0" ] || { echo "timeline switch target is not at a segment boundary: $fork_lsn"; exit 19; }

i=0
while :; do
    caught_up="$(run_as_postgres "$BIN/psql -h '$CANDIDATE_SOCKET' -p 55443 -d postgres -Atc \"SELECT pg_last_wal_replay_lsn() >= '$fork_lsn'::pg_lsn\"")"
    [ "$caught_up" = "t" ] && break
    i=$((i + 1))
    [ "$i" -lt 240 ] || { echo "candidate did not replay through $fork_lsn"; exit 20; }
    sleep 0.25
done

# Promotion may recycle parent-timeline segments, so retain their local copies.
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -m immediate -w stop" >/dev/null
cat >>"$CANDIDATE/postgresql.auto.conf" <<EOF
primary_conninfo = ''
EOF
chown postgres:postgres "$CANDIDATE/postgresql.auto.conf"
cp "$CANDIDATE"/pg_wal/00000001* "$WAL_ARCHIVE/"

run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -m fast -w stop" >/dev/null
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -l '$ROOT/candidate.log' -w start" >/dev/null
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -w promote" >/dev/null

run_as_postgres "$BIN/psql -h '$CANDIDATE_SOCKET' -p 55443 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(50001, 51000) AS g\"" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$CANDIDATE_SOCKET' -p 55443 -D '$BRANCH3' -X stream -R --checkpoint=fast" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$CANDIDATE_SOCKET' -p 55443 -D '$BRANCH4' -X stream -R --checkpoint=fast" >/dev/null
chmod 700 "$BRANCH3" "$BRANCH4"
timeline2_end="$(run_as_postgres "$BIN/psql -h '$CANDIDATE_SOCKET' -p 55443 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"

cat >>"$VOTER/postgresql.conf" <<EOF
port = 55444
listen_addresses = ''
unix_socket_directories = '$VOTER_SOCKET'
hot_standby = on
restore_command = ''
recovery_target_timeline = 'latest'
recovery_target_lsn = '$fork_lsn'
recovery_target_action = 'pause'
EOF
cat >>"$VOTER/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=$CANDIDATE_SOCKET port=55443 user=postgres dbname=postgres'
EOF
chown postgres:postgres "$VOTER/postgresql.conf" "$VOTER/postgresql.auto.conf"
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -l '$ROOT/voter.log' -w start" >/dev/null

i=0
while :; do
    replay_paused="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc 'SELECT pg_is_wal_replay_paused()'")"
    [ "$replay_paused" = "t" ] && break
    i=$((i + 1))
    [ "$i" -lt 240 ] || { echo "voter did not pause recovery at $fork_lsn"; exit 29; }
    sleep 0.25
done

i=0
while :; do
    received_timeline2="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc \"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$timeline2_end'::pg_lsn, false)\"")"
    [ "$received_timeline2" = "t" ] && break
    i=$((i + 1))
    [ "$i" -lt 240 ] || { echo "voter did not receive timeline 2 through $timeline2_end"; exit 27; }
    sleep 0.25
done

# Fence the external WAL source before every lwaldump call. PostgreSQL keeps
# the last received timeline in shared memory after walreceiver has stopped.
cat >>"$VOTER/postgresql.auto.conf" <<EOF
primary_conninfo = ''
EOF
chown postgres:postgres "$VOTER/postgresql.auto.conf"
run_as_postgres "$BIN/pg_ctl -D '$VOTER' reload" >/dev/null
i=0
while :; do
    receivers="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc 'SELECT count(*) FROM pg_stat_wal_receiver'")"
    [ "$receivers" = "0" ] && break
    i=$((i + 1))
    [ "$i" -lt 80 ] || { echo "voter WAL receiver did not stop"; exit 28; }
    sleep 0.25
done

cat >>"$BRANCH3/postgresql.conf" <<EOF
port = 55445
listen_addresses = ''
unix_socket_directories = '$BRANCH3_SOCKET'
hot_standby = on
restore_command = ''
recovery_target_timeline = 'current'
EOF
cat >>"$BRANCH3/postgresql.auto.conf" <<EOF
primary_conninfo = ''
EOF
chown postgres:postgres "$BRANCH3/postgresql.conf" "$BRANCH3/postgresql.auto.conf"
run_as_postgres "$BIN/pg_ctl -D '$BRANCH3' -l '$ROOT/branch3.log' -w start" >/dev/null
run_as_postgres "$BIN/pg_ctl -D '$BRANCH3' -w promote" >/dev/null
run_as_postgres "$BIN/psql -h '$BRANCH3_SOCKET' -p 55445 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload VALUES (51001, repeat(md5('branch3'), 12))\"" >/dev/null
timeline3_end="$(run_as_postgres "$BIN/psql -h '$BRANCH3_SOCKET' -p 55445 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"
run_as_postgres "$BIN/pg_ctl -D '$BRANCH3' -m fast -w stop" >/dev/null

# Make the second promotion allocate timeline 4 while remaining on timeline 2.
# The resulting timelines 3 and 4 are real sibling branches.
cp "$BRANCH3/pg_wal/00000003.history" "$BRANCH4/pg_wal/"
chown postgres:postgres "$BRANCH4/pg_wal/00000003.history"
cat >>"$BRANCH4/postgresql.conf" <<EOF
port = 55446
listen_addresses = ''
unix_socket_directories = '$BRANCH4_SOCKET'
hot_standby = on
restore_command = ''
recovery_target_timeline = 'current'
EOF
cat >>"$BRANCH4/postgresql.auto.conf" <<EOF
primary_conninfo = ''
EOF
chown postgres:postgres "$BRANCH4/postgresql.conf" "$BRANCH4/postgresql.auto.conf"
run_as_postgres "$BIN/pg_ctl -D '$BRANCH4' -l '$ROOT/branch4.log' -w start" >/dev/null
run_as_postgres "$BIN/pg_ctl -D '$BRANCH4' -w promote" >/dev/null
run_as_postgres "$BIN/psql -h '$BRANCH4_SOCKET' -p 55446 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload VALUES (51002, repeat(md5('branch4'), 12))\"" >/dev/null
timeline4_end="$(run_as_postgres "$BIN/psql -h '$BRANCH4_SOCKET' -p 55446 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"
run_as_postgres "$BIN/pg_ctl -D '$BRANCH4' -m fast -w stop" >/dev/null

[ "$(awk 'END { print $1 }' "$BRANCH3/pg_wal/00000003.history")" = "2" ] || exit 31
[ "$(awk 'END { print $1 }' "$BRANCH4/pg_wal/00000004.history")" = "2" ] || exit 32
cp "$BRANCH3"/pg_wal/00000003* "$VOTER/pg_wal/"
chown postgres:postgres "$VOTER"/pg_wal/00000003*

switchpoint="$(awk 'NR == 1 { print $2 }' "$VOTER/pg_wal/00000002.history")"
switch_offset="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc \"SELECT pg_wal_lsn_diff('$switchpoint', '0/0')::numeric % pg_size_bytes(current_setting('wal_segment_size'))\"")"
[ "$switch_offset" = "0" ] || { echo "timeline switchpoint is not at a segment boundary: $switchpoint"; exit 26; }

result="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'")"
timeline="${result%%|*}"
flush_lsn="${result#*|}"
covers_timeline3="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc \"SELECT '$flush_lsn'::pg_lsn >= '$timeline3_end'::pg_lsn\"")"

echo "RECOVERY_PATH_OUTPUT=$result"
echo "TIMELINE2_END=$timeline2_end"
echo "TIMELINE3_END=$timeline3_end"
echo "TIMELINE4_END=$timeline4_end"
echo "COVERS_TIMELINE3=$covers_timeline3"
[ "$timeline" = "3" ] || exit 21
[ "$covers_timeline3" = "t" ] || exit 22

# receivedTLI=2 does not reveal whether PostgreSQL has already selected 3.
# When another child of 2 appears, either 3 or 4 can be the actual target.
cp "$BRANCH4"/pg_wal/00000004* "$VOTER/pg_wal/"
chown postgres:postgres "$VOTER"/pg_wal/00000004*
if sibling_output="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'" 2>&1)"; then
    echo "expected sibling recovery paths to be rejected"
    exit 33
fi
case "$sibling_output" in
    *"could not determine recovery path"*) ;;
    *) echo "$sibling_output"; exit 34 ;;
esac
echo "SIBLING_RECOVERY_PATH_OUTPUT=$sibling_output"

# Now make timeline 4 a child of 3 and add a newer history unrelated to 2.
# PostgreSQL may still target 2, or may already have advanced through 3 to 4;
# the startup process is the only process that knows which ordering occurred.
cp "$VOTER/pg_wal/00000003.history" "$VOTER/pg_wal/00000004.history"
printf '3\t%s\tdescendant timeline\n' "$timeline3_end" >>"$VOTER/pg_wal/00000004.history"
printf '1\t%s\tunrelated newest timeline\n' "$switchpoint" >"$VOTER/pg_wal/00000005.history"
chown postgres:postgres "$VOTER/pg_wal/00000004.history" "$VOTER/pg_wal/00000005.history"
if newest_output="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'" 2>&1)"; then
    echo "expected an incompatible newest timeline to be rejected"
    exit 35
fi
case "$newest_output" in
    *"could not determine recovery path"*) ;;
    *) echo "$newest_output"; exit 36 ;;
esac
echo "INCOMPATIBLE_NEWEST_OUTPUT=$newest_output"

# PostgreSQL has already parsed this postmaster GUC with base 0. Verify that
# the extension consumes the parsed numeric target instead of parsing it again.
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -m fast -w stop" >/dev/null
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -m immediate -w stop" >/dev/null
cat >>"$VOTER/postgresql.auto.conf" <<EOF
recovery_target_timeline = '0x2'
EOF
chown postgres:postgres "$VOTER/postgresql.auto.conf"
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -l '$ROOT/voter.log' -w start" >/dev/null
numeric_result="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'")"
numeric_timeline="${numeric_result%%|*}"
numeric_flush_lsn="${numeric_result#*|}"
numeric_covers_timeline2="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc \"SELECT '$numeric_flush_lsn'::pg_lsn >= '$timeline2_end'::pg_lsn\"")"

echo "NUMERIC_RECOVERY_TARGET_OUTPUT=$numeric_result"
[ "$numeric_timeline" = "2" ] || exit 23
[ "$numeric_covers_timeline2" = "t" ] || exit 30

# Without the child's copy of the segment that contains the fork, only the
# parent up to the switchpoint is reachable. Never read its divergent tail.
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -m immediate -w stop" >/dev/null
child_switch_segment="$(find "$VOTER/pg_wal" -maxdepth 1 -type f \
    -name '00000002*' ! -name '*.history' | sort | head -n 1)"
[ -n "$child_switch_segment" ] || exit 24
mv "$child_switch_segment" "$child_switch_segment.saved"
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -l '$ROOT/voter.log' -w start" >/dev/null
fallback_result="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'")"

echo "MISSING_CHILD_SWITCH_SEGMENT_OUTPUT=$fallback_result"
echo "SWITCHPOINT=$switchpoint"
[ "$fallback_result" = "1|$switchpoint" ] || exit 25
