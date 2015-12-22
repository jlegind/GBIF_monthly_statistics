import pyhs2


def make_connection(delimiter, host, user, pw, database):

    conn = pyhs2.connect(host=host,
                   user=user,
                   database=database,
                   password=pw,
                   port=10000,
                   authMechanism="PLAIN")

    cur = conn.cursor()
        # Show databases
    print cur.getDatabases()
    return cur, conn


def run_query(sql, cur, conn):
            print sql[1]
            sql = sql[0] % sql[1]
            print sql
            cur.execute(sql)
            # Return column info from query
            # print cur.getSchema()
            # Fetch table results
            yield cur.fetch()
            # conn.close()
