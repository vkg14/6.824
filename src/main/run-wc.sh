#!/usr/bin/env bash

#
# Runs map reduce word count
#
echo '***' Starting MapReduce with wc.so.
rm -f mr-*

TIMEOUT=timeout
TIMEOUT+=" -k 2s 180s "

$TIMEOUT ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &

# wait for the coordinator to exit.
wait $pid
