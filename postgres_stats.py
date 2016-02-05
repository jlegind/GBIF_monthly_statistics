"""Makes the connection object and runs the query using the cursor"""
import psycopg2


def make_connection(database, user, password, host):
    try:
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=5432)
        cur = conn.cursor()
        return cur, conn
    except:
        print "can't connect"


def run_query(sql, cur, conn):
    """Runs the query and checks for tuple. Non-tuple allows for queries having no parameters"""
    if isinstance(sql, tuple):
        cur.execute(sql[0], sql[1])
        print "Tuple run_query runnin!"
    else:
        cur.execute(sql)
    try:
        res = cur.fetchone()
        while res:
            yield res
            res = cur.fetchone()
    except Exception,e:
        print e

