"""The launch module fires off the query prepared in the run_nnnn modules
and writes to csv file. Uses run_query() from the *_stats modules"""
# import hive_stats, postgres_stats, mysql_stats
import  postgres_stats
import csv
import creds


def prep_sql(start, end):
    #add start and end dates to SQL
    sql_country_downloads = ("SELECT u.settings -> 'country' AS country, sum(od.total_records), count(od.key), count(DISTINCT u.username) FROM public.user u " \
                            "JOIN occurrence_download od ON od.created_by = u.username " \
                            "WHERE date(od.created) BETWEEN '{start_date}' AND '{end_date}' " \
                            "AND od.status = 'SUCCEEDED' AND od.notification_addresses !~* '.*@gbif.org' " \
                            "AND od.created_by != 'nagios' GROUP BY 1 ORDER BY 3 DESC".format(start_date=start, end_date=end))

    sql_overall_disagregated = ("select 'Total records: ', sum(d.total_records) from occurrence_download d where date(d.created) BETWEEN '{start_date}' AND '{end_date}' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' AND filter IS NOT NULL " \
                            "union all " \
                            "select 'Download events: ', count(distinct d.created_by) from occurrence_download d where date(d.created) BETWEEN '{start_date}' AND '{end_date}' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' AND filter IS NOT NULL " \
                            "union all " \
                            "select 'Users: ', count(distinct d.key) from occurrence_download d where date(d.created) BETWEEN '{start_date}' AND '{end_date}' and d.status = 'SUCCEEDED' " \
                            "AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' AND filter IS NOT NULL".format(start_date=start, end_date=end))
    # print(sql_country_downloads)
    return sql_overall_disagregated, sql_country_downloads


# def launch(sql, cur, csv_location, conn, statfunc=postgres_stats):
def launch(sql, csv_location, conn, statfunc=postgres_stats):
    hv = statfunc.run_query(sql, conn)
    with open(csv_location, 'a') as csvfile:
        # my_writer = csv.writer(csvfile, delimiter='\t')
        my_writer = csv.writer(csvfile, delimiter='\t', lineterminator='\n')

        for j in hv:
            print('is type [[[[[[[', type(j))
            if isinstance(j[0], list):
                print('Its a LIST!!!!!111', j[0])
                break

                [my_writer.writerow(k) for k in j]
            else:
                my_writer.writerows(j)
    return csv_location

conn = postgres_stats.make_connection('prod_b_registry', creds.postgres_user, creds.postgrespw, 'pg1.gbif.org')
sql = prep_sql('2019-01-01', '2019-12-31')

print('len prepsql = ', len(sql))

for x in sql:
    print('x is - ', x)
    file_location = launch(x, '/home/jan/Documents/monthlystats/endofyear2019.csv', conn)
    print(file_location)
    # postgres_stats.run_query(x, conn)
