import datetime
import sql
import dbapi


def p2_pay_set(dt):
    dbapi.execute(sql.p2_pay_set_monthly['add_partition'] % {'ym': datetime.datetime.strftime(dt, "%Y%m")})
    dbapi.execute(sql.p2_pay_set_monthly['insert'] % {'ym': datetime.datetime.strftime(dt, "%Y%m")})
