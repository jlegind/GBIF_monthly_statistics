import psycopg2

try:
    conn = psycopg2.connect(database='prod_b_registry', user='', password='', host='pg1.gbif.org', port=5432)

except:
    print 'cant connect'

cur = conn.cursor()
print type(cur)
cur.execute("select sum(d.total_records) from occurrence_download d where date(d.created) BETWEEN '2015-01-01' AND '2015-08-31' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%@gbif.org' AND created_by != 'nagios' "
            "union all "
            "select count(distinct d.created_by) from occurrence_download d where date(d.created) BETWEEN '2015-01-01' AND '2015-08-31' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%@gbif.org' AND created_by != 'nagios' "
            "union all "
            "select count(distinct d.key) from occurrence_download d where date(d.created) BETWEEN '2015-01-01' AND '2015-08-31' and d.status = 'SUCCEEDED' AND d.notification_addresses NOT LIKE '%@gbif.org' AND created_by != 'nagios' ")

res = cur.fetchone()
while res:
    print res
    res = cur.fetchone()
cur.close()
conn.close()
