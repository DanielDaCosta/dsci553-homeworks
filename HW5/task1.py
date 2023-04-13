from blackbox import BlackBox
from functools import partial
import math
import random
import binascii
import csv
import sys
import time

# where k is the number of hash functions, m is the size of the Bloom filter in bits, and n is the number of elements to be added to the filter.
SIZE_BLOOM_FILTER = 69997
PREVIOUS_SET = set()
# n = 100
# k = math.ceil((SIZE_BLOOM_FILTER / n) * math.log(2))
k = 10
bit_array = [0] * SIZE_BLOOM_FILTER

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
    list_p = [next_prime(random.randint(n_users, n_users + 10000000)) for _ in range(n_hash_functions)]


    list_of_hash_functions = []
    for a, b, p in zip(list_a, list_b, list_p):

        list_of_hash_functions.append(partial(hash_function, a=a, b=b, p=p, m=n_users))

    return list_of_hash_functions

def myhashs(user_id: str) -> list:
    '''Return list of hash values

    Args:
        user_id (str)
        (global) list_of_hash_functions
    Returns:
        (list)
    '''
    result = []
    user_id_int = convert_to_int(user_id)

    for function in list_of_hash_functions:
        result.append(function(user_id_int))
    return result


def convert_to_int(user_id: str):
   return int(binascii.hexlify(user_id.encode('utf8')), 16)


# def compute_fpr(actual: set, predicted: set):
#     # Compute true positives
#     tp = len(actual & predicted)

#     # Compute false positives
#     fp = len(predicted - actual)

#     # Compute false negatives
#     fn = len(actual - predicted)

#     # Compute total number of items
#     total = len(actual.union(predicted))

#     tn = total - (tp + fp + fn)
#     if fp + tn == 0:
#         fpr = 0
#     else:
#         # Compute false positive rate
#         fpr = fp / (fp + tn)

#     return fpr


def bloom_filter(stream_users: list) -> float:
    # int_users_dict = {user: convert_to_int(user) for user in stream_users}
    # set_of_duplicated_users = set()
    global bit_array
    fp = 0
    tn = 0
    for user in stream_users:
        one_indices = set([i for i, x in enumerate(bit_array) if x == 1])
        indices_users = set(myhashs(user))

        # Intersection of elems with 1
        intersection = one_indices & indices_users
        # print(f'Intersection: {len(intersection)} OneIndices: {len(one_indices)} IndicesUsers: {len(indices_users)}')

        # if len(intersection) == 0:

        if user not in PREVIOUS_SET: # new user
            if (len(intersection) == len(indices_users)): # if user is in bloom filter
                # print('INSIDE') 
                fp += 1
            else: # if not in bloom filter
                tn += 1

        # elif len(intersection) == len(one_indices):
        #     set_of_duplicated_users.add(user)
        # else:
        #     print(len(intersection))
        new_elems = indices_users - one_indices
        for i in new_elems:
            bit_array[i] = 1

        PREVIOUS_SET.add(user)
        # set_of_duplicated_users.add(user)

        # for hashed_value_idx in myhashs(user_id):
        #     count_bits_not_present = 0
        #     one_indices & 
        #     if bit_array[hashed_value_idx]: # if not present
        #         count_bits_not_present += 1
        #     bit_array[hashed_value_idx] = 1
    # print(set_of_duplicated_users)
    # FPR = compute_fpr(PREVIOUS_SET, set_of_duplicated_users)  
        
    FPR = fp/(fp+tn)
    return FPR


def save_csv(rows: dict, output_file_name: str, header=['Time', 'FPR']):
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([header])
        # Write the data rows
        for key, value in rows.items():
            writer.writerow([key, value])

global list_of_hash_functions
list_of_hash_functions = hash_function_builder(k, SIZE_BLOOM_FILTER)


if __name__ == '__main__':
    start_time = time.time()
    dict_results = {}
    bit_array = [0] * SIZE_BLOOM_FILTER

    # Read Arguments
    if len(sys.argv) != 5:
        print("Invalid Arguments")
        exit(1)
    input_file_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_ask = int(sys.argv[3])
    output_file_path = sys.argv[4]


    bx = BlackBox()
    # users = []
    for batch in range(num_of_ask):
        # print(_)
        stream_users = bx.ask(input_file_path, stream_size)
        fpr = bloom_filter(stream_users)
        dict_results[batch] = fpr
    save_csv(dict_results, output_file_path)
    end_time = time.time()
    print('Duration: ', end_time - start_time)
