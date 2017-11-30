from pyspark import SparkConf, SparkContext

config = SparkConf().setAppName("Spark HW Question 1")
sc = SparkContext(conf=config)

google_books = sc.textFile("googlebooks-data")
booksmap = google_books.map(lambda line: (line.split("\t")[0], int(line.split("\t")[2])))

booksmap.reduceByKey(lambda v1, v2: v1 + v2).saveAsTextFile("output1")
