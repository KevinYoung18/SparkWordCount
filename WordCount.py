import re
import pandas as pd
from pyspark import SparkContext as sc
rdd = sc.textFile('KJBible.txt').flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (re.compile('[\W_]+').sub('', word).lower(), 1)) \
    .reduceByKey(lambda a, b: a + b).map(lambda x: (x[1] , x[0])) \
    .sortByKey(ascending = False).map(lambda x: (x[1] , x[0]))

df = pd.DataFrame(rdd.collect())
print(df)