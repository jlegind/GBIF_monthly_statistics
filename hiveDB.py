"""Makes the connection object and runs the query using the cursor"""
from impala.dbapi import connect


def make_connection(delimiter, host, user, pw, database):
    print('#making connection#')
    conn = connect(host=host,
                           user=user,
                           database=database,
                           password=pw,
                           port=10000,
                           # authMechanism="NOSASL")
                           auth_mechanism="PLAIN"
                   )

    cur = conn.cursor()
    return cur, conn


def run_query(sql, cur, conn):
    """sql comes as a tuple, one part is sql template, the other part is the parameters"""
    sql = sql[0] % sql[1]
    print(sql)
    cur.execute(sql)
    # Fetch table results
    yield cur.fetchall()


