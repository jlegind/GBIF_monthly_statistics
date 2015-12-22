import pyhs2
import csv


with open("/home/jan/Documents/test1.csv", 'w') as csvfile:
    my_writer = csv.writer(csvfile, delimiter = '\t')

    with pyhs2.connect(host='prodmaster1-vh',
                       port=10000,
                       authMechanism="PLAIN",
                       user='',
                       password='',
                       database='prod_b') as conn:

        with conn.cursor() as cur:
            #Show databases
            print cur.getDatabases()

            cur.execute("SELECT * FROM occurrence_multimedia LIMIT 10")

            #Return column info from query
            print cur.getSchema()

            #Fetch table results
            for i in cur.fetch():
                print i , type(i)
                if type(i) == dict:
                    pass
                else:
                    #row = '\t'.join(i)
                    my_writer.writerow(i)
                    #print '\t'.join(i)