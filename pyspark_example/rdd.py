import numpy as np
import pandas as pd
import pyspark as psp


def run():

    #readRDD()
    #mapRDDtoPandas()
    #reduceSimple()
    #wordCountMapReduce()
    #transformationExamples()
    actionExamples()

    return


def readRDD():

    sc = psp.SparkContext.getOrCreate()

    # load data from array
    d = sc.parallelize([1, 2, 3, 4])
    head(d)

    ## load data from text file
    d = sc.textFile(r'resources\test01.txt')
    head(d)

    ## load big file witn number of partitions the dataset is divided into
    d = sc.textFile(r'resources\VS14MORT.txt', 5)
    head(d)

    return


def mapRDDtoPandas():

    sc = psp.SparkContext.getOrCreate()

    d  = cs.textFile(r'resources\VS14MORT.txt')
    df = pd.DataFrame(d.map(lambda x: x.split(';')[:]).top(10))
    print(df.head())

    return


def reduceSimple():

    sc = psp.SparkContext.getOrCreate()
    d = cs.parallelize([1, 3, 0, -2, 4, 1, 2])
    print(d.reduce(max))
    print(d.reduce(min))

    return


def wordCountMapReduce():

    sc = psp.SparkContext.getOrCreate()
    d = cs.textFile(r'resources\word-count.txt')
    head(d)

    rd = d.flatMap(lambda x: x.split()) \
          .map(lambda w: (w, 1)) \
          .reduceByKey(lambda x, y: x + y)

    head(rd, top=30)

    return


def transformationExamples():

    sc = psp.SparkContext.getOrCreate()
    d1 = cs.parallelize([('a', 1), ('b', 2), ('c', 3)])
    d2 = cs.parallelize([('a', 1), ('b', 3), ('a', 4), ('d', 2)])

    d = d1.join(d2)
    print(d.collect())

    d = d1.leftOuterJoin(d2)
    print(d.collect())

    d = d.repartition(4)
    print(d.glom().collect())

    return


def actionExamples():

    sc = psp.SparkContext.getOrCreate()
    d = sc.parallelize([[1, 2, 0, 5, 1, 1, 4],
                        [1, 0, 1, 4, 1, 2, 2],
                        [0, 0, 0, 0, 3, 2, 0],
                        [0, 2, 1, 5, 3, 1, 4]])

    print(d.take(2))

    print(d.takeSample(True, 10, 9))

    print(d.collect())

    print(d.reduce(lambda x, y: np.array(x) + np.array(y)))

    d.foreach(print)

    return


def head(d, top=10):
    for item in d.top(top):
        print(item)
    print('count:', d.count())
