#!/bin/bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable
EXE_DIR=$(dirname $0)
PG_REGRESS=${PG_REGRESS:-pg_regress}
TEST_SCHEDULE=${TEST_SCHEDULE:-}
TESTS=${TESTS:-}

if [[ -z ${TESTS} ]]; then
    if [[ -z ${TEST_SCHEDULE} ]]; then
        for t in ${EXE_DIR}/sql/*.sql; do
            t=${t##${EXE_DIR}/sql/}
            t=${t%.sql}
            TESTS="${TESTS} $t"
        done
    else
        PG_REGRESS_OPTS="${PG_REGRESS_OPTS} --schedule=${TEST_SCHEDULE}"
    fi
else
    # Both this and pg_isolation_regress.sh use the same TESTS env var to decide which tests to run.
    # Since we only want to pass the test runner the kind of tests it can understand,
    # and only those which actually exist, we use TESTS as a filter for the test folder,
    # passing in only those tests from the directory which are found in TESTS
    FILTER=${TESTS}
    TESTS=
    for t in ${EXE_DIR}/sql/*.sql; do
        t=${t##${EXE_DIR}/sql/}
        t=${t%.sql}
        if [[ $FILTER = *"$t"* ]]; then
            TESTS="${TESTS} $t"
        fi
    done
fi

${PG_REGRESS} $@ ${PG_REGRESS_OPTS} ${TESTS}
