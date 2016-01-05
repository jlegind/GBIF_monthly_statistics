import postgres_stats
import launcher
import creds

cur, conn = postgres_stats.make_connection(database='prod_b_registry', user=creds.postgres_user, password=creds.postgrespw, host='pg1.gbif.org')


def pg_stats(start, end, csv_file, sql=None):
        #print type(args)
        if sql:
                sql = (sql, {'start':start, 'end':end})
        else:
                sql = (("select sum(d.total_records) from occurrence_download d where date(d.created) BETWEEN %(start)s AND %(end)s and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' "
                        "union all "
                        "select count(distinct d.created_by) from occurrence_download d where date(d.created) BETWEEN %(start)s AND %(end)s and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' "
                        "union all "
                        "select count(distinct d.key) from occurrence_download d where date(d.created) BETWEEN %(start)s AND %(end)s and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' "),
                       {'start':start, 'end':end})

        launcher.launch(sql, cur, csv_file, conn, statfunc=postgres_stats)

#
# for j in postgres_stats.run_query(sql, cur, conn):
#     #print j[0], type(j)
#     print '\t'.join(map(str,j))

