import run_postgres, run_hive, run_mysql

pg_csv = "/home/jan/Documents/test_postgres.csv"
# csv_list = []
runner = run_postgres.pg_stats('2015-10-01', '2015-11-30', pg_csv)
# # csv_list.append(runner)
# runner = run_postgres.pg_stats('2015-10-01', '2015-11-30', pg_csv, sql="SELECT * FROM node LIMIT 4")
# tt = run_postgres

#
# my_csv = "/home/jan/Documents/test_mysql.csv"
# runner = run_mysql.my_stats(my_csv)

my_csv = "/home/jan/Documents/copytest.csv"
runner = run_postgres.pg_stats('','', my_csv, sql=("CREATE UNLOGGED TABLE jan_temp"
                                                   "(user_name varchar(80),"
                                                   "country varchar(150),"
                                                   "iso_code char(5))"))
#runner = run_postgres.pg_stats('','', my_csv, sql="SELECT * FROM jan_temp")
f = open('/home/jan/Documents/test_mysql.csv', 'rw')
run_postgres.cur.copy_from(f ,'jan_temp')
#runner = run_postgres.pg_stats('','', my_csv, sql="SELECT * FROM jan_temp LIMIT 10 ")
sql=("SELECT tmp.country, tmp.iso_code, sum(dod.number_records) AS records_total, count(DISTINCT od.key) AS events, count(DISTINCT od.created_by) AS users "
                "FROM occurrence_download od "
                "JOIN dataset_occurrence_download dod ON od.key = dod.download_key "
                "JOIN (SELECT DISTINCT user_name, country, iso_code FROM jan_temp WHERE user_name != 'nagios') tmp ON od.created_by = tmp.user_name "
                "WHERE od.status = 'SUCCEEDED' AND od.notification_addresses NOT LIKE '%%@gbif.org' AND date(od.created) BETWEEN %(start)s AND %(end)s "
                "GROUP BY 1,2 ORDER BY 4 DESC")

runner = run_postgres.pg_stats('2015-01-01','2015-11-30', my_csv, sql=sql)

#
# hive_csv = "/home/jan/Documents/test_hive%s.csv"
# # runner = run_hive.hive_stats('2015-10-01', '2015-11-30', hive_csv)
# runner = run_hive.hive_stats('2015-10-01', '2015-11-30', hive_csv)

