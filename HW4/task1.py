from pyspark import SparkContext
import time
import json
import sys
import random
import math
import itertools
import csv
from functools import partial
import itertools
from graphframes import *
from pyspark.sql import SparkSession
import os


def compute_similarity(business_1: list, business_2: list, threshold: int) -> float:
    '''Compute the Jaccard similarity between lists
    Args:
        business_1 (list)
        business_2 (list)
        threshold (int)
    Returns:
        (float)
    '''
    business_1 = set(business_1)
    business_2 = set(business_2)
    return len(business_1.intersection(business_2)) >= threshold

def save_output(rows: list, output_path: str):
    with open(output_path, 'w') as f:
        f.write( '\n'.join(', '.join(f"'{value}'" for value in key_value) for key_value in rows))
        f.write('\n')
    

if __name__ == '__main__':

    # Read Arguments
    if len(sys.argv) != 4:
        print("Invalid Arguments")
        exit(1)
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    output_file_name = sys.argv[3]

    # Start SparkContext
    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR')
    sparkSession = SparkSession(sc)


    # Read File, skipping header
    review = sc.textFile(input_file_path).zipWithIndex().filter(lambda x: x[1] > 0).\
        map(lambda line: line[0].split(","))

    # Map business and users to idx
    business_idx = review.map(lambda x: x[1]).distinct().zipWithIndex()
    users_idx = review.map(lambda x: x[0]).distinct().zipWithIndex()
    index_business = business_idx.map(lambda x: (x[0], x[1])).collectAsMap()
    index_users = users_idx.map(lambda x: (x[0], x[1])).collectAsMap()
    index_users_invert = {value: key for key, value in index_users.items()}


    # Format dataset to (g)
    review_idx = review.map(lambda x: (index_users[x[0]], index_business[x[1]]))

    # Convert rdd to format [user_id, [business_id_1, business_id_2...]]
    # Filter out users who have reviewed less than threshold business, since there will be no edges for these users
    user_business = review_idx.groupByKey().mapValues(list).\
        filter(lambda x: len(x[1]) >= filter_threshold).collectAsMap()

    # Generate pairs
    list_of_pairs = itertools.combinations(user_business.keys(), 2)

    vertices_list = set() # avoid duplicates
    edges_list = set() # need to add edges in both directions because the Edge dataframe in GraphFrames expects directed edges 
    for pair in list_of_pairs:
        user_1 = index_users_invert[pair[0]]
        user_2 = index_users_invert[pair[1]]
        if compute_similarity(user_business[pair[0]], user_business[pair[1]], filter_threshold):
            # Add vertices
            vertices_list.add((user_1,))
            vertices_list.add((user_2,))

            # Add edges
            edges_list.add((user_1, user_2))
            edges_list.add((user_2, user_1))
    # Convert the list of tuples to an RDD
    vertices = sc.parallelize(vertices_list)
    edges = sc.parallelize(edges_list)

    # Convert the RDD to a DataFrame with column names 'id' and 'name'
    #vertices_df = vertices.toDF(['id', 'name'])
    vertices_df = vertices.toDF(['id'])
    edges_df = edges.toDF(['src', 'dst'])

    #####################
    # Label Propagation #
    #####################
    g = GraphFrame(vertices_df, edges_df)
    result = g.labelPropagation(maxIter=5)


    # result should be firstly sorted by the size of communities in ascending order,
    # and then the first user_id in the community in lexicographical order (the user_id is of type string).
    # The user_ids in each community should also be in the lexicographical order.
    communities = result.rdd.map(lambda x: (x[1], x[0])).groupByKey().\
    mapValues(lambda x: sorted(x)).\
    sortBy(lambda x: (len(x[1]), x[1])).map(lambda x: x[1])
        
    # print(len(communities.collect()))
    save_output(communities.collect(), output_file_name)
    end_time = time.time()
    print('Duration: ', end_time - start_time)