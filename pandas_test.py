import pandas
import numpy as np
import random

ss = pandas.Series([1,3,5,np.nan,6,8])

#print ss

mydates = pandas.date_range('20150201', periods=4)

print mydates

#df = pandas.DataFrame(np.random.randn(4,5), index=mydates, columns=list('ABCDE'))
df = pandas.DataFrame(np.random.randn(4,4))
df2 = pandas.DataFrame(np.random.randn(2,3))
print(df)
print df2

dflist = []
dflist.append(df)
dflist.append(df2)

merger = pandas.concat(dflist, axis=1)
print merger

for i in [random.randint(1,40) for j in range(5)]: print i,

print "\nrock"
