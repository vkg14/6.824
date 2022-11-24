#!/usr/bin/env bash

# run the test in a fresh sub-directory.
echo "Timing MapReduce with Word Count"
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*
echo "Setting up output directory: $(pwd)"

# Build wc, coordinator, and worker
echo "Building apps..."
(cd ../../mrapps && go build -buildmode=plugin wc.go) || exit 1
(cd .. && go build mrcoordinator.go) || exit 1
(cd .. && go build mrworker.go) || exit 1

hyperfine -w 3 'timeout -k 2s 900s ../run-wc.sh' &
pid=$!
wait $pid
