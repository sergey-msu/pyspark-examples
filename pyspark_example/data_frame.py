import numpy as np
import pyspark as psp
from pyspark.sql.types import *


def run():

    #json_df()
    #querying_df()
    airport_example()

    return


def json_df():
    sc = psp.SparkContext.getOrCreate()
    d = sc.parallelize((
             """{
                    "id": "123",
                    "name": "Katie",
                    "age": 19,
                    "eyeColor": "brown"
                 }""",
             """{
                    "id": "234",
                    "name": "Michael",
                    "age": 22,
                    "eyeColor": "green"
                }""",
             """{
                    "id": "345",
                    "name": "Simone",
                    "age": 23,
                    "eyeColor": "blue"
             }"""))
    spark = psp.sql.SparkSession.builder.appName('test-app').getOrCreate()    with spark:
        df = spark.read.json(d)
        print(df.printSchema())
        print(df.show())
        print(df.count())

        df.createOrReplaceTempView('testJson')
        res = spark.sql('select * from testJson where age>20').show()
        print(res)

    return


def querying_df():
    sc = psp.SparkContext.getOrCreate()

    with psp.sql.SparkSession.builder.appName('testApp').getOrCreate() as spark:
        d = sc.parallelize([(123, 'Katie',   19, 'brown'),
                            (234, 'Michael', 22, 'green'),
                            (345, 'Simone',  23, 'blue')])
        df = spark.createDataFrame(d, schema=['id', 'name', 'age', 'eye_color'])
        print(df.show())
        print(df.printSchema())

        # way 1
        print(df.select('id', 'age').filter('age > 20').show())

        # way 2
        print(df.select(df.id, df.age).filter(df.age > 20).show())

        # way 3
        df.createOrReplaceTempView('tempView')
        print(spark.sql('select id, age from tempView where age>20').show())

    return


def airport_example():

    sc = psp.SparkContext.getOrCreate()

    with psp.sql.SparkSession.builder.appName('airportApp').getOrCreate() as spark:

        airports_df = spark.read.csv(r'resources\airport-codes-na.txt',
                                     header=True,
                                     inferSchema='true',
                                     sep='\t')
        airports_df.createOrReplaceTempView('airports')

        flights_df = spark.read.csv(r'resources\departure-delays.csv', header=True)
        flights_df.createOrReplaceTempView('flights')

        flights_df.cache()

        print(airports_df.show())
        print(flights_df.show())

        result = spark.sql('select a.City, f.origin, sum(f.delay) as Delays '+
                           'from airports as a '+
                           'join flights as f '+
                               'on a.IATA=f.origin '+
                           'where a.state = \'WA\' '+
                           'group by a.City, f.origin '+
                           'order by sum(f.delay) desc')
        print(result.show())

    return