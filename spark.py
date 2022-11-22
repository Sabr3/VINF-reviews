import os
import glob
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from multiprocessing.pool import ThreadPool

start_time = time.time()
json_file_list = ["data/part-01.json", "data/part-02.json", "data/part-03.json", "data/part-04.json",
                  "data/part-05.json", "data/part-06.json"]
spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "9g").appName('SparkByExamples.com')\
        .getOrCreate()


def parse_data(filename):

    # Initial read of json files
    df = spark.read.option("multiline", "true").json(filename)

    cnt = df.count()

    # Drop column review_summary
    df = df.drop('review_summary')

    # Reliability summer
    df2 = df.withColumn("helpful", col("helpful")[0] - col("helpful")[1])

    # Replace "/" in reviewer's name to "|"
    df2 = df2.withColumn("reviewer", expr('replace(reviewer, "/", "|")'))

    # When rating null - put 5.0 as a default value
    df2 = df2.withColumn('rating', when(col('rating').isNull(), 5).otherwise(col('rating')))

    # Transform spoiler_tag column name to isSpoiler, review_id to id and review_detail to review
    df2 = df2.withColumnRenamed('spoiler_tag', 'spoiler')
    df2 = df2.withColumnRenamed('review_id', 'id')
    df2 = df2.withColumnRenamed('review_detail', 'review')

    # Movie release year add column and remove year
    df2 = df2.withColumn("movie_release_year", when(col('movie').contains('\u2013'), substring("movie", -7, 5)).otherwise(
        substring("movie", -5, 4)))
    df2 = df2.withColumn("movie", expr('replace(movie, coalesce(movie_release_year, ""), "")'))
    df2 = df2.withColumn("movie", expr('replace(movie, " ()", "")'))
    df2 = df2.withColumn("movie", expr('replace(movie, " ( )", "")'))

    # Transform date to date format from string
    df2 = df2.withColumn("review_date",
                         when(col('review_date').contains('January'), expr('replace(review_date, '
                                                                           '" January ", "-1-")')).
                         when(col('review_date').contains('February'), expr('replace(review_date, '
                                                                            '" February ", "-2-")')).
                         when(col('review_date').contains('March'), expr('replace(review_date, '
                                                                         '" March ", "-3-")')).
                         when(col('review_date').contains('April'), expr('replace(review_date, '
                                                                         '" April ", "-4-")')).
                         when(col('review_date').contains('May'), expr('replace(review_date, '
                                                                       '" May ", "-5-")')).
                         when(col('review_date').contains('June'), expr('replace(review_date, '
                                                                        '" June ", "-6-")')).
                         when(col('review_date').contains('July'), expr('replace(review_date, '
                                                                        '" July ", "-7-")')).
                         when(col('review_date').contains('August'), expr('replace(review_date, '
                                                                          '" August ", "-8-")')).
                         when(col('review_date').contains('September'), expr('replace(review_date, '
                                                                             '" September ", "-9-")')).
                         when(col('review_date').contains('October'), expr('replace(review_date, '
                                                                           '" October ", "-10-")')).
                         when(col('review_date').contains('November'), expr('replace(review_date, '
                                                                            '" November ", "-11-")')).
                         when(col('review_date').contains('December'), expr('replace(review_date, '
                                                                            '" December ", "-12-")')))

    out_filename = filename.lstrip('data/').rstrip('.json')
    # Create one jsonlines file from spark dataframe
    df2.coalesce(1).write.format('json').mode('overwrite').save('sparked_data/' + out_filename)

    spark_dir = os.getcwd() + '/sparked_data/' + out_filename + '/'
    filenames = glob.glob(spark_dir + '*.json')
    os.rename(filenames[0], os.getcwd() + '/sparked_data/' + out_filename + '.jl')

    print(filename + ": " + str(cnt) + " rows")


# MultiThreading - If needed change the thread count to lower number
pool = ThreadPool(8)
pool.map(parse_data, json_file_list)

# Clean the files
for i in range(6):
    shutil.rmtree(os.getcwd() + '/sparked_data/part-0' + str(i+1))

print("--- %s seconds ---" % (time.time() - start_time))
