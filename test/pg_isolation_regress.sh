#!/bin/bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable
EXE_DIR=$(dirname $0)
PG_ISOLATION_REGRESS=${PG_ISOLATION_REGRESS:-pg_isolation_regress}
ISOLATION_TEST_SCHEDULE=${ISOLATION_TEST_SCHEDULE:-}
TESTS=${TESTS:-}

if [[ -z ${TESTS} ]]; then
    if [[ -z ${ISOLATION_TEST_SCHEDULE} ]]; then
        for t in ${EXE_DIR}/specs/*.spec; do
            t=${t##${EXE_DIR}/specs/}
            t=${t%.spec}
            TESTS="${TESTS} $t"
        done
    else
        PG_ISOLATION_REGRESS_OPTS="${PG_ISOLATION_REGRESS_OPTS} --schedule=${ISOLATION_TEST_SCHEDULE}"
    fi
else
    FILTER=${TESTS}
    TESTS=
    for t in ${EXE_DIR}/specs/*.spec; do
        t=${t##${EXE_DIR}/specs/}
        t=${t%.spec}
        if [[ $FILTER = *"$t"* ]]; then
            TESTS="${TESTS} $t"
        fi
    done
fi

${PG_ISOLATION_REGRESS} $@ ${PG_ISOLATION_REGRESS_OPTS} ${TESTS}
