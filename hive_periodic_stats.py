"""Makes the connection object and runs the query using the cursor
Runs Python 3.6 """
from impala.dbapi import connect
# import thriftpy
import csv

#CLI parameters that can be included in bash chron job or other
# start_date = sys.argv[1]
# end_date = sys.argv[2]

snapshot_yyyymmdd = '20201001'

#takes a yyyymmdd variable as the snapshot occurrence table
sql_prod = '''SELECT t1.publishingcountry, t2.publisher_country, t1.ct as Nov11_count, t2.ct as beginning_year, t1.ct - t2.ct as delta FROM
(
SELECT count(o.gbifid) as ct, publishingcountry FROM prod_h.occurrence o
GROUP BY publishingcountry
)t1
JOIN
(
SELECT count(id) as ct, publisher_country FROM snapshot.occurrence_{}
GROUP BY publisher_country)t2
ON t1.publishingcountry = t2.publisher_country ORDER BY delta DESC'''.format(snapshot_yyyymmdd)
#{} in string is the variable placeholder for format()



def make_hive_connection(db, user, pw):
    '''
    Make an object that queries HIVE
    :param db: string db name
    :return: HQL query gateway cursor
    '''
    conn_prod = connect(host='c5master2-vh',
                           user='jlegind',
                           database=db,
                           password='jlegind',
                           port=10000,
                           # authMechanism="NOSASL")
                           auth_mechanism="PLAIN"
                   )

    return conn_prod.cursor()

sql_snap_test = 'SELECT s.dataset_id, s.publisher_id FROM snapshot.occurrence_20201001 s LIMIT 10'

def run_query(sql, db_name, user, pw):

    cursor = make_hive_connection(db_name, user, pw)
    cursor.execute(sql)
    # Fetch table results
    return cursor.fetchall()


# hive = make_connection("\t", 'c5master2-vh', 'jlegind', 'jlegind', 'prod_h')
# sql = 'SELECT specieskey FROM occurrence LIMIT 10'
res = run_query(sql_snap_test, 'prod_h', 'jlegind', 'jlegind')
# for j in res:
#     print(j)
print(res)

with open('stats.csv', 'w', newline='', encoding='utf-8') as stats:
    field_names = ['datasetkey', 'pubkey']
    writer = csv.writer(stats, delimiter='\t')
    writer.writerow(field_names)
    writer.writerows(res)
