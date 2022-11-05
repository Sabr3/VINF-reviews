import os
import glob
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

MONTH_DICT = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
              'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

sc = pyspark.SparkContext('local[*]')
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.option("multiline", "true").json(["sample.json", "part-06.json"])

print(f'Number of lines: {df.count()}\n\n')
print(f'Columns: {df.columns}\n\n')

# Drop column review_summary
df = df.drop('review_summary')
print(f'Columns after dropping: {df.columns}\n\n')

# Reliability summer
df2 = df.withColumn("helpful", col("helpful")[0] - col("helpful")[1])

# When rating null - put 5.0 as a default value
df2 = df2.withColumn('rating', when(col('rating').isNull(), 5).otherwise(col('rating')))

# Transform spoiler_tag column name to isSpoiler, review_id to id and review_detail to review
df2 = df2.withColumnRenamed('spoiler_tag', 'spoiler')
df2 = df2.withColumnRenamed('review_id', 'id')
df2 = df2.withColumnRenamed('review_detail', 'review')

# Movie release year add column and remove year
df2 = df2.withColumn("movie_release_year", substring("movie", -5, 4))
df2 = df2.withColumn("movie", expr('replace(movie, coalesce(movie_release_year, ""), "")'))
df2 = df2.withColumn("movie", expr('replace(movie, " ()", "")'))

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

# Create one jsonlines file from spark dataframe
df2.coalesce(1).write.format('json').mode('overwrite').save('sparked_data')
spark_dir = os.getcwd() + '/sparked_data/'
filenames = glob.glob(spark_dir + '*.json')
os.rename(filenames[0], spark_dir + 'data.jl')
