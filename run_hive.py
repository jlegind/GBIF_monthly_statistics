import hive_stats
import launcher

cur, conn = hive_stats.make_connection("\t", 'prodmaster1-vh', '', '', 'prod_b')

def hive_stats(start, end, csv_file):
    sql = (("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) BETWEEN '%(start)s' AND '%(end)s' "
            "GROUP BY publishingcountry ORDER BY CT DESC"),
           {'start':start, 'end':end})
    launcher.launch(sql, cur, csv_file % 1, conn)

    sql = (("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) <= '%(end)s' "
            "GROUP BY publishingcountry ORDER BY CT DESC"),
           {'end':end})
    launcher.launch(sql, cur, csv_file % 2, conn)


    #
    # sql = (("SELECT * FROM occurrence_multimedia LIMIT %(end)s"),
    #        {'end':end})
    # launcher.launch(sql, cur, csv_file % 2, conn)
