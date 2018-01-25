import datetime
import sql
import dbapi


def p1_pay_set(dt):
    dbapi.execute(sql.p1_pay_set['add_partition'] % {'dt': datetime.datetime.strftime(dt, "%Y%m%d")})
    dbapi.execute(sql.p1_pay_set['insert'] % {'dt': datetime.datetime.strftime(dt, "%Y%m%d"),
                                              'dashed-dt': datetime.datetime.strftime(dt, "%Y%m%d")})


def p2_pay_set(dt):
    dbapi.execute(sql.p2_pay_set_daily['add_partition'] % {'dt': datetime.datetime.strftime(dt, "%Y%m%d")})
    dbapi.execute(sql.p2_pay_set_daily['insert'] % {'dt': datetime.datetime.strftime(dt, "%Y%m%d")})
