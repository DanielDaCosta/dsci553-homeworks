import math
from blackbox import BlackBox
import random
from functools import partial
from collections import defaultdict
import csv
import statistics
import binascii
import sys
import time

N_HASH_FUNC = 200
N_PARTITIONS = 4 #math.ceil(N_HASH_FUNC/4)
_STREAM_SIZE = 600


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


def convert_to_int(user_id: str):
   return int(binascii.hexlify(user_id.encode('utf8')), 16)

def myhashs(user_id: str) -> list:
    '''Return list of hash values

    Args:
        user_id
        (global) list_of_hash_functions
    Returns:
        (list)
    '''
    result = []
    user_id_int = convert_to_int(user_id)

    for function in list_of_hash_functions:
        result.append(bin(function(user_id_int))) # Return binary
    return result

def compute_trailing_zeros(user_hashed_values: list, hash_dict: dict) -> None:
    '''Return number of trailing zeros per hash function

    Args:
        user_hashed_values(list[str])
        hash_dict (dict): stores number of trailing zeros per hash function
    Returns:
        (None)
    '''

    # max_trailing_zeros = 0
    user_remove_0b_list = [user[2:] for user in user_hashed_values] # remove '0b'
    for i, user_remove_0b in enumerate(user_remove_0b_list):
        hash_dict[i].append(len(user_remove_0b) - len(user_remove_0b.rstrip('0')))
        # if trailing_zeros > max_trailing_zeros:
            # max_trailing_zeros  = trailing_zeros


def combine_estimates(R_per_hash_func: list, n_partitions: int) -> int:
    '''Partition hash functions into small groups -> 
    Take average for each group -> ake the median of the averages

    Args:
        R_per_hash_func (list)
        n_partitions (int)
    Return:
        (int)
    '''
    
    size_rows = math.floor(len(R_per_hash_func)/n_partitions)
    # print(len(R_per_hash_func))

    results = []
    # Partition hash functions into small groups
    for index, i in enumerate(range(0, len(R_per_hash_func), size_rows)):
        split = R_per_hash_func[i: i + size_rows]
        # Take average for each group
        results.append(sum(split)/len(split))


    return int(statistics.median(results))


def flajolet_martin_algorithm(stream_users: list, n_partitions: int) -> int:
    hash_dict = defaultdict(list) # stores number of trailing zeros per hash function
    for user in stream_users:
        # Get binary hashes
        hashed_values = myhashs(user)

        # For user
        compute_trailing_zeros(hashed_values, hash_dict) # modifies hash_dict

    max_2R = [2**max(n_zeros) for _, n_zeros in hash_dict.items()]
    # print(max_2R)

    estimate_2R = combine_estimates(max_2R, n_partitions)
    return estimate_2R


def save_csv(rows: dict, output_file_name: str, header=['Time', 'Ground Truth', 'Estimation']):
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([header])
        # Write the data rows
        for key, value in rows.items():
            writer.writerow([key, value[0], value[1]])


global list_of_hash_functions
list_of_hash_functions = hash_function_builder(N_HASH_FUNC, _STREAM_SIZE)


if __name__ == '__main__':
    start_time = time.time()
    dict_results = {}

    # Read Arguments
    if len(sys.argv) != 5:
        print("Invalid Arguments")
        exit(1)
    input_file_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_ask = int(sys.argv[3])
    output_file_path = sys.argv[4]

    bx = BlackBox()
    for batch in range(num_of_ask):
        # print(_)
        stream_users = bx.ask(input_file_path, stream_size)
        estimate = flajolet_martin_algorithm(stream_users, N_PARTITIONS)
        # myhashs()
        # users.extend(stream_users)
        # fpr = bloom_filter(stream_users)
        unique_users = len(set(stream_users))
        dict_results[batch] = [unique_users, estimate]

    save_csv(dict_results, output_file_path)
    end_time = time.time()
    print('Duration: ', end_time - start_time)
