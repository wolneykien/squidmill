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
print_log_record()
{
    printf '%s.000\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
	   "$1" "$ELAPSED" "$CLIENT" "$ACTION_CODE" "$SIZE" \
	   "$METHOD" "$URI" "$IDENT" "$FROM" "$CONTENT"
}

# Outputs the specified number of records.
# Optionally, a starting timestamp value (int) can
# be specified (defaults to 0).
#
# args: number-of-records [start-timestamp]
print_log()
{
    local max=$1
    local i=0
    local t=${2:-0}

    echo "Write $max records into the test log file" >&2    
    while [ $i -lt $max ]; do
        print_log_record $t
	i=$((i + 1))
	t=$((t + 1))
    done
}

# Counts the records in the given test log file.
# Outputs 0 if the file doesn't exist.
#
# args: log-file
count_log()
{
    if [ -f "$1" ]; then
	cat "$DIR/$PREFIX.access.log" | wc -l
    else
	echo 0
    fi
}

# Compares written count of records to the given count.
# If no additional arguments are given checks if more than 0
# records were written to the file.
#
# args: log-file [cmp expected-count]
check_written()
{
    local count=$(count_log "$1")
    local op=${2:--gt}
    local expected=${3:-0}

    [ $count $op $expected ]
}

# Compares written count of records to the given count.
# If no additional arguments are given checks if more than 0
# records were written to the file.
# Outputs the information messages.
#
# args: log-file [cmp expected-count]
assert_written()
{
    local count=$(count_log "$1")
    local op=${2:--gt}
    local expected=${3:-0}

    if [ $count $op $expected ]; then
	echo "The file $1 contains $count records"
	return 0
    else
	echo "Error: the file $1 contains $count records"
	return 1
    fi
}

LIBGAMBC_ARGS=-:daq-
# Calls squidmill with the specified arguments.
# Additional $LIBGAMBC_ARGS options are added.
#
# args: [squidmill-args]
run_squidmill()
{
    "$SQUIDMILL" $LIBGAMBC_ARGS "$@"
}

DEFAULT_TIMEOUT=10
# Waits for the PID to be written into the
# given PID-file and returns its value.
# Optionally a timeout in seconds can be
# specified. Default is $DEFAULT_TIMEOUT.
#
# args: pidfile [timeout]
read_pid()
{
    echo "Reading the pidfile: $1" >&2

    sleep ${2:-$DEFAULT_TIMEOUT} &
    local pid=$!

    tail -n +0 --pid=$pid -F "$1" | ( grep -m 1 '^[0-9]\+$' && kill -PIPE $pid )
    if wait $pid; then
	echo "Unable to read the PID" >&2
	return 1
    else
	echo "PID has been read" >&2
    fi
}

# Terminates a squidmill background process
# with the specified PID. Optionally a timeout
# value can be specified.
#
# args: pid [timeout]
terminate_squidmill()
{
    local timeout=${2:-$DEFAULT_TIMEOUT}
    timeout=$((timeout * 5))
    local i=0

    if kill $1 2>/dev/null; then
	echo "Wait for squidmill to terminate..."
	while [ $i -lt $timeout ] && kill -0 $1 2>/dev/null; do
	    sleep 0.2
	    i=$((i + 1))
	done
	if [ $i -eq $timeout ] && kill -0 $1 2>/dev/null; then
	    echo "Time is up: squidmill is still running"
	    return 1
	else
	    echo "Squidmill finished"
	fi
    else
	echo "Squidmill already finished"
    fi
}

# Queries the DB using the sqlite3 command.
#
# args: DB-filename SQL-query [column-number-to-cut]
query_db()
{
    sqlite3 -bail -batch -cmd "$2" -cmd ".quit" "$1" | if [ -n "${3:-}" ]; then cut -d '|' -f $3; else cat; fi
}

# Compares the record count in all of the specified tables
# with the given number.
#
# args: db-file cmp expected-count table-name [table-name...]
check_db_count()
{
    local db="$1"; shift
    local op="$1"; shift
    local expected="$1"; shift
    local db_count=0
    local table_count

    echo "Count the DB ($@) records"

    for t in "$@"; do
	table_count=$(query_db "$db" "select count(*) from $t")
	db_count=$((db_count + table_count))
    done

    if [ $db_count $op $expected ]; then
	echo "Count OK"
	return 0
    else
	echo "Count error: found $db_count records, expected $expected"
	return 1
    fi
}

# Compares the sum of the given column in all of the specified
# tables with the given number.
#
# args: db-file col cmp expected-sum table-name [table-name...]
check_db_sum()
{
    local db="$1"; shift
    local col="$1"; shift
    local op="$1"; shift
    local expected="$1"; shift
    local db_sum=0
    local table_sum

    echo "Sum the DB ($@) by the '$col' column"

    for t in "$@"; do
	table_sum=$(query_db "$db" "select sum($col) from $t")
	db_sum=$((db_sum + table_sum))
    done

    if [ $db_sum $op $expected ]; then
	echo "Sum OK"
	return 0
    else
	echo "Sum error: sum is $db_sum, expected $expected"
	return 1
    fi
}

# Checks if a record with the specified timestamp was
# passed into the squidmill DB by analysing the squidmill
# debug log file.
#
# args: debug-log-filename timestamp
timestamp_passed()
{
    cat "$1" | grep -q "^\"insert or ignore into access_log select $2.000,"
}

# Waits for the record with the specified timestamp to be
# passed into the squidmill DB by analysing the squidmill
# debug log file. Optionally a timeout in seconds can be
# specified. Default is $DEFAULT_TIMEOUT.
#
# args: debug-log-filename timestamp [timeout]
wait_for_timestamp()
{
    echo "Waiting for the record $2 to be passed into the DB..."

    sleep ${3:-$DEFAULT_TIMEOUT} &
    local pid=$!

    tail -n +0 --pid=$pid -F "$1" | ( grep -q -m 1 "^\"insert or ignore into access_log select $2.000," && kill -PIPE $pid )
    if wait $pid; then
	echo "Record hasn't been passed"
	return 1
    else
	echo "Record has been passed"
	return 0
    fi
}
