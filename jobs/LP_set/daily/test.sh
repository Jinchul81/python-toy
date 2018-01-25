#!/bin/sh
export HOME_JOBS="${HOME}/workspace/dbp_pilot/jobs"
source ${HOME_JOBS}/library/bash/loader.sh
export HOME_JOB="${HOME_JOBS}/LP_set/daily"

function run_daily
{
  export TARGET_DATABASE="loc"
  export END_DT=$1

  run_python "${HOME_JOB}/runner.py"
}

function run_monthly
{
  curr_dt=20170501
  end_dt=20170528
  while [ ${curr_dt} -le ${end_dt} ]
  do
    run_daily "${curr_dt}"
    curr_dt=$((curr_dt+1))
  done
}

run_daily "20170508"
#run_monthly
