import hiveDB as hv
import csv
import creds
import re


def hive_sql(start, end, filename, target_dir='/home/jan/MonthlyStatsWeb/stats/jekyll-now/_data/', db='prod_f'):

    hhive = hv.make_connection("\t", 'c5master2-vh', creds.hive_user, creds.hivepw, db)
    sql_inyear = (("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) BETWEEN '%(start)s' AND '%(end)s' "
                "GROUP BY publishingcountry ORDER BY CT DESC"),
               {'start':start, 'end':end})

    sql_total = (("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) < '%(end)s' "
                "GROUP BY publishingcountry ORDER BY CT DESC"),
               {'end':end})

    def run_query(sql, target_file):
        res = hv.run_query(sql, hhive[0], hhive[1])
        print(target_file)

        with open(target_file, 'w+') as tfile:
            csv_writer = csv.writer(tfile, delimiter=',', lineterminator='\n')
            csv_writer.writerow(('Count', 'Country'))
            for j in res:
                for k in j:
                    print(k)
                    list(k).reverse()
                    csv_writer.writerow(k)

    filename = filename.replace(' ','')

    if re.search('fromstart', filename):
        # tests if it is "from start of year" or "country totals for all time"
        print("Runs from start of year")
        run_query(sql_inyear, '{}/{}.csv'.format(target_dir, filename))
    elif re.search('countrytotal', filename):
        print("Runs totals")
        run_query(sql_total, '{}/{}.csv'.format(target_dir, filename))
    else:
        raise ValueError("Unexpected csv file name!")
