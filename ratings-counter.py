import sys
from pyspark import SparkContext, SparkConf
import collections
if __name__ == "__main__":
    conf = SparkConf().setMaster("local")\
        .setAppName("ratings-counter")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(sys.argv[1])
    linesRDD = lines.repartition(2)
    # print(lines)
    ratingsRDD = linesRDD.map(lambda x: x.split()[2])
    # print(ratings.getNumPartitions())
    #results = ratingsRDD.countByValue()
    ratingsCOl = ratingsRDD.collect()
    for row in ratingsCOl:
        print(row[0])
    #sortedResultsRDD = collections.OrderedDict(sorted(results.items()))
    #print(sortedResultsRDD)
