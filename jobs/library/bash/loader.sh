#!/bin/sh

if [ -z "${HOME_GRADLE}" ]; then
  export HOME_GRADLE="/opt/gradle"
  export PATH="${HOME_GRADLE}/bin:${PATH}"
fi

if [ -z `which gradle 2>&1 | grep -v "no gradle"` ]; then
  echo "Not found gradle"
  return 1
fi

if [ -z "${PYTHONPATH}" ]; then
  export PYTHONPATH=".:${HOME_JOBS}/library/python"
fi

function run_python
{
  retval=1
  script_path=$1
  python ${script_path} && retval=0
  if [ ${retval} -eq 0 ]
  then
    echo "Successfully finished: $0"
  else
    echo "Failed: $0"
  fi
  return ${retval}
}
