from pyspark import SparkContext
import time
import json
import sys
import random
import math
import itertools
import csv
from functools import partial

N_HASH_FUNCTIONS = 100
B_BANDS = 30

def next_prime(num):
    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(n ** 0.5) + 1):
            if n % i == 0:
                return False
        return True

    n = num + 1
    while True:
        if is_prime(n):
            return n
        n += 1


def hash_function(x:int, a: int, b: int, p: int, m: int) -> int:
    return ((a*x + b) %p) % m

def hash_function_builder(n_hash_functions: int, n_users: int) -> list:
    # Config Variables
    # range_values = sys.maxsize - 1
    # p = 233333333333 # prime_number bigger than n_users
    

    # random.seed(0) # set seed for reproducibility
    list_a = [random.randint(1, n_users) for _ in range(n_hash_functions)]
    # random.seed(42) # set seed for reproducibility
    list_b = [random.randint(1, n_users) for _ in range(n_hash_functions)]
    # random.seed(45) # set seed for reproducibility
    list_p = [next_prime(random.randint(n_users, n_users + 10000)) for _ in range(n_hash_functions)]


    list_of_hash_functions = []
    for a, b, p in zip(list_a, list_b, list_p):

        list_of_hash_functions.append(partial(hash_function, a=a, b=b, p=p, m=n_users))

    
    return list_of_hash_functions

def jaccard_similarity(business_1: list, business_2: list) -> float:
    '''Compute the Jaccard similarity between lists

    Args:
        business_1 (list)
        business_2 (list)

    Returns:
        (float)
    '''
    business_1 = set(business_1)
    business_2 = set(business_2)
    return len(business_1.intersection(business_2))/len(business_1.union(business_2))

def divide_into_bands(signature_list: list, n_bands: int) -> list:
    '''Divide column from signature matrix into n_bands

    Args:
        signature_list (list)
        n_bands (list)

    Returns:
        (list): list of list
    '''
    size_rows = math.floor(len(signature_list)/n_bands)
    # print(size_rows)

    for index, i in enumerate(range(0, len(signature_list), size_rows)):
        yield (index, hash(tuple(signature_list[i: i + size_rows])))

def compute_candidate_similarity(business_users_list: dict, threshold: float, candadidate_pairs, reversed_business_id_index_dict) -> set:
    '''Compute original jaccard similarity of candidate pairs
    '''
    result = []
    similar_business = set()
    tmp_initial_length = 0
    for candidate in candadidate_pairs:
        tmp_jacc_sim = jaccard_similarity(business_users_list[candidate[0]], business_users_list[candidate[1]])
        if tmp_jacc_sim >= threshold:            
            similar_business.add(frozenset(candidate))
            if len(similar_business) != tmp_initial_length:
                tmp_initial_length += 1
                result.append([reversed_business_id_index_dict[candidate[0]], reversed_business_id_index_dict[candidate[1]], tmp_jacc_sim])

    return result

def save_csv(rows: list, output_file_name: str, header=['business_id_1', 'business_id_2', 'similarity']):
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([header])
        writer.writerows(rows)


if __name__ == '__main__':

    # Read Arguments
    if len(sys.argv) != 3:
        print("Invalid Arguments")
        exit(1)
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    start_time = time.time()
    # Start SparkContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # Read File, skipping header
    review = sc.textFile(input_file_path).zipWithIndex().filter(lambda x: x[1] > 0).\
        map(lambda line: line[0].split(","))

    ######################
    # Data Preprocessing #
    ######################
    # Generating an index for each of the users
    user_id_index = review.map(lambda x: x[0]).distinct().zipWithIndex()
    n_users = user_id_index.count()
    user_id_index_dict = user_id_index.collectAsMap()

    # Generating an index for each of the business
    business_id_index = review.map(lambda x: x[1]).distinct().sortBy(lambda x: x).zipWithIndex()
    business_id_index_dict = business_id_index.collectAsMap()

    reversed_business_id_index_dict = {business_id_index: business_id for business_id, business_id_index in business_id_index_dict.items()}

    # Generate list of hash functions
    list_of_hash_functions = hash_function_builder(N_HASH_FUNCTIONS, n_users)
    # Apply hash_functions to user_id
    # Output: [(user_id, [h1, h2, h3, ...])]
    user_id_index_hashed = user_id_index.map(lambda x: (user_id_index_dict[x[0]], x[1]))\
        .mapValues(lambda x: [ hash_func(x) for hash_func in list_of_hash_functions])

    business_id_with_user_id = review.map(lambda x: (user_id_index_dict[x[0]], business_id_index_dict[x[1]])).distinct()

    # Get list of hashed(users_id) who liked a business
    # [ (business_id_1, [hash_1(user_id_1), hash_2(user_id_1)]), (business_id_1, [hash_1(user_id_2), hash_2(user_id_2)])..)]
    business_id_with_user_id_index =  business_id_with_user_id.join(user_id_index_hashed).map(lambda x: (x[1][0], x[1][1]))

    # Combine business_id per hash functions (h1, h2...)
    # [ ((business_id, 0), h0(user_id_1)), ((business_id, 0), h0(user_id_2))]
    # (business_id, 0) => 0 means h0, 1 means h1,....

    business_id_per_hash_function_per_user_id = business_id_with_user_id_index.\
        flatMap(lambda x: [((x[0], idx), elem) for idx, elem in enumerate(x[1])])
    # Find minimum hash(user_id) per hash function
    # Output: [(business_id, ( 0, min_h0)), (business_id, (1, min_h1))]
    business_id_per_hash_function = business_id_per_hash_function_per_user_id.reduceByKey(min).\
        map(lambda x: (x[0][0], (x[0][1], x[1])))
    
    # Build Signature Matrix sorting each (business, [list of hashes]) by the hash_function_number (h0, h1, ..)
    signature_matrix = business_id_per_hash_function.groupByKey().map(
        lambda business_id_hash_list: (business_id_hash_list[0], sorted(list(set(business_id_hash_list[1])), key=lambda x: x[0]))
    ).map(lambda x: (x[0], [user_hash_idx for (hash_function_number, user_hash_idx) in x[1]]))

    #######
    # LSH #
    #######
    # Divide matrix into b bands and r rows => Two items are a candidate pair
    # if their signatures are identical in at least one band.

    # Divide signature matrix into chunks
    signature_matrix_chunks = signature_matrix\
        .flatMap(lambda sig_matrix_col:[(chunk, sig_matrix_col[0]) for chunk in divide_into_bands(sig_matrix_col[1], B_BANDS)])
    
    # Retrive candidates: "Two items are a candidate pair if their signatures are identical in at least one band."
    # Returns pairs and rows in lexicographical order
    candadidate_pairs = signature_matrix_chunks.groupByKey().mapValues(list).\
        filter(lambda chunk_similar: len(chunk_similar[1]) > 1).\
        map(lambda chunk_similar: sorted(chunk_similar[1])).\
            flatMap(lambda chunk_similar: [tuple(sorted(candidate_pair)) for candidate_pair in itertools.combinations(chunk_similar, 2)])\
            .distinct().sortBy(lambda x: x).collect()

    # Final results will be the candidate pairs whose original Jaccard similarity is >= 0.5
    # Business_id: [user_id_1, user_id_2..] 
    business_id_with_user_id_list = business_id_with_user_id.\
        map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).collectAsMap()

    similar_items = compute_candidate_similarity(business_id_with_user_id_list, 0.5, candadidate_pairs, reversed_business_id_index_dict)
    save_csv(similar_items, output_file_path)
    end_time = time.time()
    print('Duration: ', end_time - start_time)

