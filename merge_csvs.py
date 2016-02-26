"""Merges the output from monthly_stats into a csv file"""

import pandas
import fileinput as fi


def into_csv(files, column_header):
    """Accepts a list of csv files and a list of headers
    These will be nice formatted into one csv file"""
    #The files and columnns lists need to be in the same order

    dflist = []
    column_header.reverse()
    for j in files:
        cols = column_header.pop()
        df = pandas.read_csv(j, sep="\t", header=None, names=cols)
        df['###'] = pandas.DataFrame([''])
        #Creates spacing between the individual tables
        dflist.append(df)

    merger = pandas.concat(dflist, axis=1)
    #Combines the data frames into one while allowing for unequal number of rows
    merger = merger.astype(object)
    #Converts floats to objects, because of scientific formatting suppression of large numbers
    myfile = '/home/jan/Documents/pre-final.csv'
    merger.to_csv(myfile, sep='\t', index=False)
    print merger
    ipt = fi.input(myfile, backup='.bak')
    writer = open(myfile.replace("pre-", ""), 'w')
    #Removes .0 from count integers that were forced into float
    for line in ipt:
        writer.write(line.replace(".0", ""))
    ipt.close()
