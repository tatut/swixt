#!env bash
./build-debug.sh
for f in *.pl; do swipl -p foreign=. -g run_tests -t halt $f; done
