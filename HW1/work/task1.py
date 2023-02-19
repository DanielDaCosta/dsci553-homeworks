from pyspark import SparkContext
import time
import json
import sys

# Read Arguments
if len(sys.argv) != 3:
    print("Invalid Arguments")
    exit(1)
review_filepath = sys.argv[1]
output_filepath = sys.argv[2]

# Start SparkContext

sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN') 

# Read File
review = sc.textFile(review_filepath)
review = review.map(json.loads)


#####
# A #
#####
n_review = review.map(lambda x: 1).reduce(lambda x,y: x + y)

#####
# B #
#####
n_review_2018 = review.filter(lambda x: x['date'][:4] == '2018').map(lambda x: 1).reduce(lambda x,y: x + y)

#####
# C #
#####
n_user = review.map(lambda x: x['user_id']).distinct().count()

#####
# D #
#####
top10_user = review.map(lambda x: (x['user_id'] , 1)).groupByKey().mapValues(sum).takeOrdered(10, lambda x: (-x[1], x[0]))

#####
# E #
#####
n_business = review.map(lambda x: x['business_id']).distinct().count()

#####
# F #
#####
top10_business = review.map(lambda x: (x['business_id'] , 1)).reduceByKey(lambda x,y: x + y ).takeOrdered(10, lambda x: (-x[1], x[0]))

# Save File
output = {
    'n_review': n_review,
    'n_review_2018': n_review_2018,
    'n_user': n_user,
    'top10_user': top10_user,
    'n_business': n_business,
    'top10_business': top10_business
}

with open(output_filepath, "w") as outfile:
    json.dump(output, outfile)