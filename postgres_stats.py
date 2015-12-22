import psycopg2


def make_connection(database, user, password, host):
    try:
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=5432)
        cur = conn.cursor()
        return cur, conn

    except:
        print "can't connect"


def run_query(sql, cur, conn):
    print type(sql)
    if isinstance(sql, tuple):
        print sql[0]
        print sql[1]
        cur.execute(sql[0], sql[1])
        print "Tuple run_query runnin!"
        #res = cur.fetchone()
    else:
        cur.execute(sql)
        #res = cur.fetchone()
    try:
        res = cur.fetchone()
        while res:
            print res
            yield res
            res = cur.fetchone()
    except Exception,e:
        print e

    # cur.close()
    # conn.close()
