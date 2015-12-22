import mysql.connector as mysqlconn

# sql = ("SELECT t1.usr, t1.country, t1.iso FROM "
# "(SELECT users.uid, users.name AS usr, field_data_field_firstname.field_firstname_value, field_data_field_lastname.field_lastname_value, "
# "users.mail, from_unixtime(users.created) AS created, from_unixtime(users.login), taxonomy_term_data.name AS country, field_data_field_iso2.field_iso2_value AS iso "
# "FROM users LEFT JOIN field_data_field_firstname ON field_data_field_firstname.entity_id = users.uid LEFT JOIN field_data_field_lastname ON field_data_field_lastname.entity_id = users.uid "
# "LEFT JOIN field_data_field_country_mono ON field_data_field_country_mono.entity_id = users.uid "
# "LEFT JOIN taxonomy_term_data ON taxonomy_term_data.tid = field_data_field_country_mono.field_country_mono_tid "
# "LEFT JOIN field_data_field_iso2 ON field_data_field_iso2.entity_id = taxonomy_term_data.tid WHERE users.login <> 0) AS t1 ORDER BY t1.country DESC LIMIT 10000 " )

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
        res = cur.fetchone()
        res = ['' if x is None else x for x in res]
        res = [x.encode('utf-8') for x in res]
        yield res
        res = cur.fetchone()
    cur.close()