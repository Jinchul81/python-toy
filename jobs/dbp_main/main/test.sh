#!/bin/sh
export HOME_JOBS="${HOME}/workspace/dbp_pilot/jobs"
source ${HOME_JOBS}/library/bash/loader.sh
export HOME_JOB="${HOME_JOBS}/dbp_main/main"

export WIND_DATABASE="src"
export LOCATION_DATABASE="loc"
export TARGET_DATABASE="comm"

run_python "${HOME_JOB}/runner.py"
