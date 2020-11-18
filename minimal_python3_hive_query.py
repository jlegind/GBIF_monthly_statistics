"""Makes the connection object and runs the query using the cursor
Runs Python 3.6
Is a proof-of-concept minimum implementation with a simple SQL statement
Please uncomment the sql_prod string to test more complex query"""
from impala.dbapi import connect

#CLI parameters that can be included in bash chron job or other
# start_date = sys.argv[1]


#takes a yyyymmdd variable as the snapshot occurrence table
#Compare snapshot_beginning_of_year to current state of prod_h.occurrence or other table by subtracting -
# - beginning of year numbers.
snapshot_yyyymmdd = '20201001'
# sql_prod = '''SELECT t1.publishingcountry, t2.publisher_country, t1.ct as Nov11_count, t2.ct as beginning_year, t1.ct - t2.ct as delta FROM
# (
# SELECT count(o.gbifid) as ct, publishingcountry FROM prod_h.occurrence o
# GROUP BY publishingcountry
# )t1
# JOIN
# (
# SELECT count(id) as ct, publisher_country FROM snapshot.occurrence_{}
# GROUP BY publisher_country)t2
# ON t1.publishingcountry = t2.publisher_country ORDER BY delta DESC'''.format(snapshot_yyyymmdd)
#{} in string is the variable placeholder for format()


connection = connect(host='c5master2-vh',user='myUser',
                       database='prod_h', password='myPW',
                       port=10000, auth_mechanism="PLAIN"
                    )
cursor = connection.cursor()

test_sql = 'SELECT s.dataset_id, s.publisher_country FROM snapshot.occurrence_20201001 s LIMIT 10'

cursor.execute(test_sql)
res= cursor.fetchall()

print(res)

