#!/bin/sh
export HOME_JOBS="${HOME}/workspace/dbp_pilot/jobs"
source ${HOME_JOBS}/library/bash/loader.sh
export HOME_JOB="${HOME_JOBS}/LP_set/weekly"

export TARGET_DATABASE="loc"
export END_DT="20170528"

run_python "${HOME_JOB}/runner.py"
