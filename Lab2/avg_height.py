from pyspark import SparkConf, SparkContext

sc = SparkContext("spark://b524f35852c2:7077", "avg_height")

# Read the CSV file and skip the header
#rdd = sc.textFile("file:///root/arbres.csv")
rdd = sc.textFile("hdfs://b524f35852c2:9000/input/arbres.csv")

header = rdd.first()  # Get the header row
rdd = rdd.filter(lambda line: line != header)  # Filter out the header

# Extract the heights and ensure they can be converted to float
heights = rdd.map(lambda line: line.split(";")[7])  # Get the 8th column (index 7)
heights = heights.filter(lambda x: x.replace('.', '', 1).isdigit())  # Ensure the value is numeric
heights = heights.map(lambda x: float(x))  # Convert to float

# Calculate the average height
avg_height = heights.mean()

print("avg tree height is: ", avg_height)

sc.stop()

