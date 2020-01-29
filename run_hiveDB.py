import hiveDB as hv
import csv
import creds
import re


def hive_sql(hiveDB_version, typesql, start, end, filename, target_dir='/home/jan/MonthlyStatsWeb/stats/jekyll-now/_data/', db='prod_g'):

    hhive = hv.make_connection("\t", 'c5master2-vh.gbif.org', creds.hive_user, creds.hivepw, hiveDB_version)
    sql_inyear = ("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) BETWEEN '{start_date}' AND '{end_date}' "
                "GROUP BY publishingcountry ORDER BY CT DESC".format(start_date=start, end_date=end))
    print(sql_inyear)


    sql_total = ("SELECT count(*) AS CT, publishingcountry FROM occurrence_hdfs WHERE to_date(from_unixtime(CAST(fragmentcreated/1000 AS int))) < '{end_date}' "
                "GROUP BY publishingcountry ORDER BY CT DESC".format(end_date=end))

    def run_query(sql, target_file):
        res = hv.run_query(sql, hhive[0], hhive[1])
        print(target_file)

        with open(target_file, 'w+') as tfile:
            print('in target file.... ')
            csv_writer = csv.writer(tfile, delimiter=',', lineterminator='\n')
            csv_writer.writerow(('Count', 'Country'))
            for j in res:
                for k in j:
                    print(k)
                    list(k).reverse()
                    csv_writer.writerow(k)

    filename = filename.replace(' ','')

    print("run-hive part", filename)

    # if re.search('fromstart', filename):
    #     # tests if it is "from start of year" or "country totals for all time"
    #     print("Runs from start of year")
    if typesql == 'inyear':
        sql = sql_inyear
    else:
        sql = sql_total
        run_query(sql, '{}/{}.csv'.format(target_dir, filename))

    # elif re.search('countrytotal', filename):
    #     print("Runs totals")
    #     run_query(sql_total, '{}/{}.csv'.format(target_dir, filename))
    # else:
    #     raise ValueError("Unexpected csv file name!")

hiveDB = 'prod_h'
hive_sql(hiveDB, 'total', '2019-01-01', '2019-12-31', 'hive_total.csv', target_dir='/home/jan/PycharmProjects/GBIF_monthly_stats/')