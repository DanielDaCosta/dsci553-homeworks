from pyspark import SparkContext
from pyspark.sql import SQLContext
import time
import json
import sys

# Read Arguments
if len(sys.argv) != 5:
    print("Invalid Arguments")
    exit(1)
review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output_filepath_question_a = sys.argv[3]
output_filepath_question_b = sys.argv[4]

# Start SparkContext

sc = SparkContext.getOrCreate()
sc1 = SQLContext(sc)

#######
## A ##
#######

# Read Files
review = sc1.read.json(review_filepath).rdd
business = sc1.read.json(business_filepath).rdd

# Discard records with empty “city”
business_filtered = business.filter(lambda x: len(x['city']) > 0)
review_business = review.map(lambda x: (x['business_id'], x['stars'])).join(business_filtered.map(lambda x: (x['business_id'], x['city'])))

# Compute Count and Sum
review_business_agg = review_business.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
output_question_a = review_business_agg.map(lambda x: (x[0], (x[1][0]/x[1][1]))).collect()

# Save File

header = 'city,stars'
with open(output_filepath_question_a,'w') as f:
    f.write(header + "\n")
    f.write( '\n'.join(','.join(str(value) for value in key_value) for key_value in output_question_a))


#######
## B ##
#######

################
# Execution M1 #
################
start_time = time.time()

# Read
review = sc1.read.json(review_filepath).rdd
business = sc1.read.json(business_filepath).rdd

# Collect
# Discard records with empty “city”
business_filtered = business.filter(lambda x: len(x['city']) > 0)
# Join RDDs
review_business = review.map(lambda x: (x['business_id'], x['stars'])).join(business_filtered.map(lambda x: (x['business_id'], x['city'])))

# Compute Count and Sum
review_business_agg = review_business.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
review_business_sorted_rdd = review_business_agg.map(lambda x: (x[0], (x[1][0]/x[1][1]))).takeOrdered(10, lambda x: (-x[1], x[0]))##collect()


exe_time_m1 = time.time() - start_time

################
# Execution M2 #
################

start_time = time.time()

# Read
review = sc1.read.json(review_filepath).rdd
business = sc1.read.json(business_filepath).rdd

# Collect
# Discard records with empty “city”
business_filtered = business.filter(lambda x: len(x['city']) > 0)
# Join RDDs
review_business = review.map(lambda x: (x['business_id'], x['stars'])).join(business_filtered.map(lambda x: (x['business_id'], x['city'])))

# Compute Count and Sum
review_business_agg = review_business.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
review_business_agg_list = review_business_agg.map(lambda x: (x[0], (x[1][0]/x[1][1]))).collect()

# Sorting in Python
review_business_sorted_python = sorted(review_business_agg_list, key=lambda x: (-x[1], x[0]))[:10]

exe_time_m2 = time.time() - start_time

# Save File
output_question_3_b = {
    "m1": exe_time_m1,
    "m2": exe_time_m2,
    "reason": "it works because.."
}

with open(output_filepath_question_b, "w") as outfile:
    json.dump(output_question_3_b, outfile)
