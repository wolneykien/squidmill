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
