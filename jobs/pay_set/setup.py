#!/usr/bin/python

import sql
import dbapi


def create_table():
    dbapi.execute(sql.P1PaySet.create())
    dbapi.execute(sql.P2PaySetDaily.create())
    dbapi.execute(sql.P2PaySetMonthly.create())


if __name__ == '__main__':
    create_table()
