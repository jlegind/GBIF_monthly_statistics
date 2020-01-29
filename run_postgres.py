import postgres_stats
import creds
import csv

conn = postgres_stats.make_connection(database='prod_b_registry', host='pg1.gbif.org', user=creds.postgres_user, password=creds.postgrespw)


def pg_stats(start, end, csv_file, sql=None, labels=None):
        print(type(start), type(end))
        # start = start+' {}'.format('00:00:00')
        # end = end+' {}'.format('00:00:00')
        if sql:
                # print("SQL true --- ", sql)
                sql = sql
        else:
                sql = ("select 'Total records downloaded: ', sum(d.total_records) from occurrence_download d where date(d.created) BETWEEN '{start}' AND '{end}' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' "
                        "union all "
                        "select 'Total number of users: ', count(distinct d.created_by) from occurrence_download d where date(d.created) BETWEEN '{start}' AND '{end}' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' AND created_by != 'nagios' "
                        "union all "
                        "select 'Total download events: ', count(distinct d.key) from occurrence_download d where date(d.created)"
       " BETWEEN '{start}' AND '{end}' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%%@gbif.org' "
       "AND created_by != 'nagios' ".format(start = start, end=end))

        # return postgres_stats.run_query(sql, cur, conn)
        res = postgres_stats.run_query(sql, conn)
        print(res)
        print('creating '+csv_file)

        with open(csv_file, 'w+') as csvfile:
            my_writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
            if labels:
                print('labels TRUE')
                my_writer.writerow(labels)
            for j in res:
                print(j)
                if isinstance(j[0], list):
                    [my_writer.writerow(k) for k in j]
                else:
                    my_writer.writerow(j)
        return csv_file