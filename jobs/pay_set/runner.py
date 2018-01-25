#!/usr/bin/python

import datetime
import sql
import dbapi
import util
import sys

reload(sys)
sys.setdefaultencoding('utf8')


@util.timeit
def partition_and_insert(set, dt):
    dbapi.execute(set.add_partition(dt))
    dbapi.execute(set.insert(dt))


def main(dt):
    partition_and_insert(sql.P1PaySet, dt)
    partition_and_insert(sql.P2PaySetDaily, dt)

    # run if last day of month
    if (dt + datetime.timedelta(days=1)).day == 1:
        partition_and_insert(sql.P2PaySetMonthly, dt)


if __name__ == '__main__':
    dt = datetime.datetime.today() - datetime.timedelta(days=1)
    if len(sys.argv) >= 2:
        dt = datetime.datetime.strptime(sys.argv[-1], "%Y%m%d")

    main(dt)
