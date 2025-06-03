from pyspark import SparkConf, SparkContext

sc = SparkContext("spark://b524f35852c2:7077", "count_lines")
#rdd = sc.textFile("file:///root/arbres.csv")

rdd = sc.textFile("hdfs://b524f35852c2:9000/input/arbres.csv")

nbr = rdd.count()

print("nbr of lines: ",nbr)

sc.stop()


