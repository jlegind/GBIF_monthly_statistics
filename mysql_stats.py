import mysql.connector as mysqlconn


def make_connection(database, user, password, host):
    try:
        conn = mysqlconn.connect(database=database, user=user, password=password, host=host)
        cur = conn.cursor()
        return cur, conn

    except:
        print "can't connect"

def run_query(sql, cur, conn):
    cur.execute(sql)

    print "run_query runnin!"
    res = cur.fetchone()
    while res:
        res = ['' if x is None else x for x in res]
        # replaces None values that don't have the str() function
        res = [str(x) if isinstance(x, int) else x for x in res]
        # sets int to string for encoding purposes
        res = [x.encode('utf-8') for x in res]
        # convert to utf-8
        yield res
        res = cur.fetchone()
    cur.close()