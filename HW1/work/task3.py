from pyspark import SparkContext
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
sc.setLogLevel('WARN') 

#######
## A ##
#######

# Read Files
review = sc.textFile(review_filepath)
review = review.map(json.loads)
business = sc.textFile(business_filepath)
business = business.map(json.loads)

# Discard records with empty “city”
business_filtered = business.filter(lambda x: len(x['city']) > 0)
review_business = review.map(lambda x: (x['business_id'], x['stars'])).join(business_filtered.map(lambda x: (x['business_id'], x['city'])))

# Compute Count and Sum
review_business_agg = review_business.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
output_question_a = review_business_agg.map(lambda x: (x[0], (x[1][0]/x[1][1]))).takeOrdered(10, lambda x: (-x[1], x[0]))

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
review = sc.textFile(review_filepath)
review = review.map(json.loads)
business = sc.textFile(business_filepath)
business = business.map(json.loads)


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

exe_time_m1 = time.time() - start_time

################
# Execution M2 #
################


start_time = time.time()

# Read
review = sc.textFile(review_filepath)
review = review.map(json.loads)
business = sc.textFile(business_filepath)
business = business.map(json.loads)

# Collect
# Discard records with empty “city”
business_filtered = business.filter(lambda x: len(x['city']) > 0)
# Join RDDs
review_business = review.map(lambda x: (x['business_id'], x['stars'])).join(business_filtered.map(lambda x: (x['business_id'], x['city'])))

# Compute Count and Sum
review_business_agg = review_business.map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
review_business_sorted_rdd = review_business_agg.map(lambda x: (x[0], (x[1][0]/x[1][1]))).takeOrdered(10, lambda x: (-x[1], x[0]))##collect()


exe_time_m2 = time.time() - start_time

# Save File
output_question_3_b = {
    "m1": exe_time_m1,
    "m2": exe_time_m2,
    "reason": ("We observe that the python sorting executed faster than Spark."
               "Even though spark process data in parallel, when sorting small datasets, "
               "it can be the case that python runs the code faster. But for huge datasets spark will outperform python")
    # "When working in a distributed environment, sorting becomes an expensive operation. "
    # "Since the rows of data are in different machines, Spark must execute shuffle operations to sort the data, which is expensive. "
    # "On the other hand, in python sorting, we are sorting a list in a single machine, leading to a higher performance.")
}

with open(output_filepath_question_b, "w") as outfile:
    json.dump(output_question_3_b, outfile)
