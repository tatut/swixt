#!/bin/sh
#PG_FLAGS="-I/opt/homebrew/include/postgresql@14 -L/opt/homebrew/lib/postgresql@14 -lpq"
##SWIPL_FLAGS=$(pkg-config --cflags --libs swipl)
##FLAGS="$PG_FLAGS $SWIPL_FLAGS"

swipl-ld -DDEBUG -g -shared -o swixt swixt.c json.c pgwire.c
