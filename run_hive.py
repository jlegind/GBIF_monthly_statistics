"""Runs the hive_stats module and launcher with the hardcoded SQL
Creds contain the credential for the DB connection"""
import hive_stats
import launcher
import creds

cur, conn = hive_stats.make_connection("\t", 'prodmaster1-vh', creds.hive_user, creds.hivepw, 'prod_b')

def hive_stats(start, end, csv_file):
    sql = (("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) BETWEEN '%(start)s' AND '%(end)s' "
            "GROUP BY publishingcountry ORDER BY CT DESC"),
           {'start':start, 'end':end})
    l1 = launcher.launch(sql, cur, csv_file % 1, conn)

    sql = (("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) <= '%(end)s' "
            "GROUP BY publishingcountry ORDER BY CT DESC"),
           {'end':end})
    l2 = launcher.launch(sql, cur, csv_file % 2, conn)
    return l1, l2
