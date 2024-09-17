from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Exception Handling and removing wrong data lines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Function - Cleaning the data
def correctRows(p):
    if len(p) == 17:  # Ensure the line has all 17 columns
        if isfloat(p[5]) and isfloat(p[11]):
            # Check trip duration, trip distance, and fare amounts
            if (float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0):
                return p

# Main function
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <input_file> <output_task1> <output_task2>", file=sys.stderr)
        exit(-1)

    # Initialize Spark Context
    sc = SparkContext(appName="Assignment-1")
    spark = SparkSession.builder.appName("Assignment-1").getOrCreate()

    # Load the dataset
    input_file = sys.argv[1]  # Path to the input file (local path)
    rdd = sc.textFile(input_file)

    # Split each line by comma
    rdd_split = rdd.map(lambda line: line.split(','))

    # Clean the data using the correctRows function
    rdd_cleaned = rdd_split.filter(correctRows)

    # Task 1: Top 10 Active Taxis (highest number of distinct drivers)
    # Key-Value RDD: (medallion, hack_license)
    taxi_rdd = rdd_cleaned.map(lambda row: (row[0], row[1])).distinct()

    # Use reduceByKey to count distinct drivers per taxi
    driver_count_rdd = taxi_rdd.mapValues(lambda x: {x}).reduceByKey(lambda a, b: a.union(b))
    driver_count_rdd = driver_count_rdd.mapValues(lambda x: len(x))

    # Get the top 10 taxis with the highest number of distinct drivers
    top_10_taxis = driver_count_rdd.takeOrdered(10, key=lambda x: -x[1])

    # Save Task 1 result as a single TXT file
    sc.parallelize(top_10_taxis).coalesce(1).saveAsTextFile(sys.argv[2])

    # Task 2: Top 10 Best Drivers (highest earnings per minute)
    # Key-Value RDD: (hack_license, (earnings_per_minute, 1))
    earnings_rdd = rdd_cleaned.map(lambda row: (row[1], (float(row[16]) / (float(row[4]) / 60), 1)))

    # Use reduceByKey to sum the earnings and count the number of trips for each driver
    total_earnings_rdd = earnings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Calculate average earnings per minute
    avg_earnings_rdd = total_earnings_rdd.mapValues(lambda x: x[0] / x[1])

    # Get the top 10 drivers with the highest average earnings per minute
    top_10_drivers = avg_earnings_rdd.takeOrdered(10, key=lambda x: -x[1])

    # Save Task 2 result as a single TXT file
    sc.parallelize(top_10_drivers).coalesce(1).saveAsTextFile(sys.argv[3])

    # Stop the Spark context
    sc.stop()
