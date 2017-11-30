from pyspark import SparkConf, SparkContext

config = SparkConf().setAppName("Spark HW Question 2")
sc = SparkContext(conf=config)

google_books = sc.textFile("googlebooks-data")
booksmap = google_books.map(lambda line: (line.split("\t")[1], 1))

booksmap.reduceByKey(lambda v1, v2: v1 + v2).saveAsTextFile("output2")
