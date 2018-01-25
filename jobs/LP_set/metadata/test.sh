#!/bin/sh
export HOME_JOBS="${HOME}/workspace/dbp_pilot/jobs"
source ${HOME_JOBS}/library/bash/loader.sh
export HOME_JOB="${HOME_JOBS}/LP_set/metadata"

export TARGET_DATABASE="loc"

run_python "${HOME_JOB}/runner.py"
