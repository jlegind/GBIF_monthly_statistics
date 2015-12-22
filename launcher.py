# The launch module fires off the query prepared in the run_nnnn modules
import hive_stats, postgres_stats, mysql_stats
import csv


def launch(sql, cur, csv_location, conn, statfunc=hive_stats):
    print csv_location
    hv = statfunc.run_query(sql, cur, conn)
    with open(csv_location, 'w') as csvfile:
        my_writer = csv.writer(csvfile, delimiter='\t')

        for j in hv:
            #print j
            if isinstance(j[0], list):
                [my_writer.writerow(k) for k in j]
            else:
                my_writer.writerow(j)
    return csv_location
