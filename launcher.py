"""The launch module fires off the query prepared in the run_nnnn modules
and writes to csv file. Uses run_query() from the *_stats modules"""
import hive_stats, postgres_stats, mysql_stats
import csv


def launch(sql, cur, csv_location, conn, statfunc=hive_stats):
    hv = statfunc.run_query(sql, cur, conn)
    with open(csv_location, 'w') as csvfile:
        my_writer = csv.writer(csvfile, delimiter='\t')

        for j in hv:
            if isinstance(j[0], list):
                [my_writer.writerow(k) for k in j]
            else:
                my_writer.writerow(j)
    return csv_location
