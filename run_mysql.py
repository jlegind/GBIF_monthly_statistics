import mysql_stats as my
import launcher
import creds

cur, conn = my.make_connection(database='drupal_live', user=creds.mysql_user, password=creds.mysqlpw, host='camelot.gbif.org')
print type(cur)
print type(conn)


def my_stats(csv_file):
    sql = ("SELECT t1.usr, t1.country, t1.iso FROM "
    "(SELECT users.uid, users.name AS usr, field_data_field_firstname.field_firstname_value, field_data_field_lastname.field_lastname_value, "
    "users.mail, from_unixtime(users.created) AS created, from_unixtime(users.login), taxonomy_term_data.name AS country, field_data_field_iso2.field_iso2_value AS iso "
    "FROM users LEFT JOIN field_data_field_firstname ON field_data_field_firstname.entity_id = users.uid LEFT JOIN field_data_field_lastname ON field_data_field_lastname.entity_id = users.uid "
    "LEFT JOIN field_data_field_country_mono ON field_data_field_country_mono.entity_id = users.uid "
    "LEFT JOIN taxonomy_term_data ON taxonomy_term_data.tid = field_data_field_country_mono.field_country_mono_tid "
    "LEFT JOIN field_data_field_iso2 ON field_data_field_iso2.entity_id = taxonomy_term_data.tid WHERE users.login <> 0) AS t1" )

    launcher.launch(sql, cur, csv_file, conn, statfunc=my)
# for j in my.run_query(sql, cur, conn):
#     print '\t'.join(map(str, j))
#     # for k in j:
#     #     print k
#     # print j[0], type(j)

