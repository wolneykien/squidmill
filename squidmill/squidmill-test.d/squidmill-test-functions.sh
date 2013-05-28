#!/bin/sh -efu

ELAPSED=100
CLIENT="127.0.0.1"
ACTION_CODE="TCP_HIT/200"
SIZE=1024
METHOD="GET"
URI="http://test.uri/test"
IDENT="-"
FROM="NONE/-"
CONTENT="text/html"

# Prints the access.log entry with parameters
# defined above and the specified timestamp value (int).
#
# args: timestamp
print_log()
{
    printf '%s.000\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
	   "$1" "$ELAPSED" "$CLIENT" "$ACTION_CODE" "$SIZE" \
	   "$METHOD" "$URI" "$IDENT" "$FROM" "$CONTENT"
}

# Queries the DB using the sqlite3 command.
#
# args: DB-filename SQL-query [column-number-to-cut]
query_db()
{
    sqlite3 -bail -batch -cmd "$2" -cmd ".quit" "$1" | if [ -n "${3:-}" ]; then cut -d '|' -f $3; else cat; fi
}

# Checks if a record with the specified timestamp was
# inserted into the squidmill DB using squidmill debug
# log file.
#
# args: timestamp debug-log-filename
timestamp_inserted()
{
    tail "$2" | grep -q "^\"insert or ignore into access_log select $1.000,"
}

# Waits for the last written timestamp to be processed by squidmill
#
# args: access-log debug-log end-time
wait_for_squidmill()
{
    local time=$(date +%s)
    local end=$3
    if [ "${end#+}" != "$end" ]; then
        end=${end#+}
	end=$((start + end))
    fi
    local last=$(tail -1 "$1" | sed -e 's/\.000.*$//')
    while [ $time -lt $end ] && ! timestamp_inserted $last "$2"; do
        sleep 0.2
	time=$(date +%s)
    done
}

# Terminates or kills squidmill
#
# args: pid end-time
terminate_squidmill()
{
    local time=$(date +%s)
    local end=$3
    if [ "${end#+}" != "$end" ]; then
        end=${end#+}
	end=$((start + end))
    fi
    while [ $time -lt $end ] && kill -0 $1 2>/dev/null; do
        sleep 0.2
	time=$(date +%s)
    done
    if kill -0 $1 2>/dev/null; then
	kill -9 $1
	return 1
    fi
}
