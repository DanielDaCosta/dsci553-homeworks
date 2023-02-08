from pyspark import SparkContext
from pyspark.sql import SQLContext
import time
import json
import sys

# Read Arguments
if len(sys.argv) != 4:
    print("Invalid Arguments")
    exit(1)
review_filepath = sys.argv[1]
output_filepath = sys.argv[2]
n_partitions_custom = int(sys.argv[3])

# Start SparkContext

sc = SparkContext.getOrCreate()
sc1 = SQLContext(sc)

# Read File
review = sc1.read.json(review_filepath).rdd

# Default Partition
n_partitions_default = review.getNumPartitions()
n_items_default = review.glom().map(lambda x: len(x)).collect()
start_time = time.time()
top10_business = review.map(lambda x: (x['business_id'] , 1)).groupByKey().mapValues(sum).takeOrdered(10, lambda x: (-x[1], x[0]))
exe_time_default = time.time() - start_time
print(f"Execution time default: {exe_time_default}")

# Custom Partition
start_time = time.time()
top10_bussiness_custom_partition = review.map(lambda x: (x['business_id'] , 1)).partitionBy(n_partitions_custom, lambda x: ord(x[:1]))
top10_business_custom = top10_bussiness_custom_partition.groupByKey().mapValues(sum).takeOrdered(10, lambda x: (-x[1], x[0]))
exe_time_custom = time.time() - start_time
n_items_custom = top10_bussiness_custom_partition.glom().map(lambda x: len(x)).collect()
print(f"Execution time custom: {time.time() - start_time}")


output = {}
output['default'] = {
    'n_partition': n_partitions_default,
    'n_items': n_items_default,
    'exe_time': exe_time_default
}

output['customized'] = {
    'n_partition': n_partitions_custom,
    'n_items': n_items_custom,
    'exe_time': exe_time_custom
}

with open(output_filepath, "w") as outfile:
    json.dump(output, outfile)
