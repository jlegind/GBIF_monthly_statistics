import run_postgres, run_hive, run_mysql, csv
import merge_csvs as mcsv
import sys


start_date = sys.argv[1]
end_date = sys.argv[2]
csv_list = []
headers = []

##The total downloads stats part
pg_csv = "/home/jan/Documents/test_postgres.csv"
runner = run_postgres.pg_stats(start_date, end_date, pg_csv)
f = open(runner, "r+")
rows = []
labels = ['Total downloaded records', 'users', 'downloads']
headers.append(['Overall download stats\n %s to %s' % (start_date, end_date),'Counts'])
#Assigns row labels
labels.reverse()
for row in csv.reader(f):
    rows.append([labels.pop()+"\t"+row[0]])
f = open(runner, "w+")
w = csv.writer(f)
w.writerows(rows)
f.close()
csv_list.append(runner)
##END

##Begin User Download for MySQL and Postgres
my_csv = "/home/jan/Documents/test_mysql.csv"
runner = run_mysql.my_stats(my_csv)

my_csv = "/home/jan/Documents/copytest.csv"
runner = run_postgres.pg_stats('','', my_csv, sql=("CREATE UNLOGGED TABLE jan_temp"
                                                   "(user_name varchar(80),"
                                                   "country varchar(150),"
                                                   "iso_code char(5))"))

f = open('/home/jan/Documents/test_mysql.csv', 'rw')
run_postgres.cur.copy_from(f ,'jan_temp')

sql=("SELECT tmp.country, tmp.iso_code, sum(dod.number_records) AS records_total, count(DISTINCT od.key) AS events, count(DISTINCT od.created_by) AS users "
                "FROM occurrence_download od "
                "JOIN dataset_occurrence_download dod ON od.key = dod.download_key "
                "JOIN (SELECT DISTINCT user_name, country, iso_code FROM jan_temp WHERE user_name != 'nagios') tmp ON od.created_by = tmp.user_name "
                "WHERE od.status = 'SUCCEEDED' AND od.notification_addresses NOT LIKE '%%@gbif.org' AND date(od.created) BETWEEN %(start)s AND %(end)s "
                "GROUP BY 1,2 ORDER BY 4 DESC")

headers.append(['Downloads by country from %s to %s\ncountry' % (start_date, end_date), 'iso_code', 'records_total', 'events', 'users'])
runner = run_postgres.pg_stats(start_date, end_date, my_csv, sql=sql)
csv_list.append(runner)
##END

##Begin indexed records stats from HIVE
hive_csv = "/home/jan/Documents/test_hive%s.csv"
runner = run_hive.hive_stats(start_date, end_date, hive_csv)
[csv_list.append(j) for j in runner]
headers.append(['Indexed record_count\nfrom %s to %s' % (start_date, end_date), 'country'])
headers.append(['Indexed record_count\nTotal up until %s' % (end_date), 'country'])

print csv_list
print headers

mcsv.into_csv(csv_list, headers)
