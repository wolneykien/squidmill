#!/bin/sh

PREFIX="${1-.}"

fill_log()
{
    t=1
    while [ $t -lt 1000 ]; do
        echo "$t.000    152 127.0.0.1 TCP_HIT/200 3349752 GET ftp://ftp.altlinux.org/pub/distributions/ALTLinux/Sisyphus/noarch/RPMS.classic/vim-common-7.2.315-alt1.noarch.rpm - NONE/- application/x-rpm" >> access.log
        sleep 0.1
        t=$(( t + 1 ))
    done
    echo $? > fill.done
}

rm -f access.log
rm -f fill.done
touch access.log
fill_log &
fill_pid=$!

mill() {
    $PREFIX/squidmill -:daq- -d squidmill.db -B 1 -F access.log -D > dbwrite.log&
    mill_pid=$!
    echo $mill_pid > mill.pid
    wait $mill_pid
    echo $? > mill.done
    rm mill.pid
}

rm -f dbwrite.log
rm -f squidmill.db
rm -f mill.pid
touch mill.pid
mill &
[ -f mill.pid ] && sleep 4

rm -f dbread.log
read_test()
{
    while [ ! -f fill.done ] && [ -f mill.pid ]; do
        $PREFIX/squidmill -:daq- -d squidmill.db -r -i -D >> dbread.log
        ret=$?
        if [ $ret -eq 0 ] || [ $ret -eq 100 ]; then
            sleep 0.2
	else
            echo "Read test failed ($ret)"
            echo "READ LOG TAIL"
            tail dbread.log
	    break
        fi
    done
    echo "Read loop done"
    if [ ! -f fill.done ]; then
        echo "Stop fill process $fill_pid"
        kill $fill_pid
    else
        echo "Fill process done"
    fi
    if [ -f mill.pid ]; then
        mill_pid=`cat mill.pid`
        echo "Stop squidmill process $mill_pid"
        kill $mill_pid
    else
        echo "Mill process done"
        ret=`cat mill.done`
	if [ $ret -ne 0 ]; then
            echo "Write test failed ($ret)"
            echo "WRITE LOG TAIL"
            tail dbwrite.log
	fi
    fi
    wait
    return $ret
}

read_test
ret=$?

if [ $ret -eq 0 ]; then    
    echo "Concurent read/write test passed"
fi
