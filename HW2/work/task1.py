from pyspark import SparkContext
import sys
from collections import defaultdict
import itertools
import time

def new_candidate(subset: tuple, L_k_1: set, L_1: set, k: int, C_k: list) -> set:
    list_of_candidates = set()
    for frequent_item in L_1: # Iterate over each item of L_1 that does not intersect
        if frequent_item[0] in subset: # Do nothing if L_1 item is already in subset
            continue
        # For each basket, we need only look at those items that are in L_1
        new_candidate = set(sorted(subset + frequent_item)) 
        
        # print(new_candidate)
        # print(list_of_candidates)
        if new_candidate not in list_of_candidates:
            # Examine each pair and determine whether or not that pair is in L_k - 1
            subset_combination = set(itertools.combinations(new_candidate, k-1))
            # print(subset_combination)
            # print(subset_combination)
            if subset_combination <= L_k_1:
                list_of_candidates.add(tuple(new_candidate)) #if new_candidate not in C_k else list_of_candidates # Remove duplicates
    return list_of_candidates


def generate_candidates(L_k_1: set, L_1: set, k: int) -> set:
    # Generatin C_k from L_k_1 and L_1
    C_k = set()
    for subset in L_k_1:
        new_subsets_candidates = new_candidate(subset, L_k_1, L_1, k, C_k)
        for new_subset in new_subsets_candidates:
            C_k.add(new_subset) #if new_subsets_candidates not in C_k else C_k
            # break

    return C_k


def count_itemsets_of_size_k(basket_rdd: list, candidates_list: list, threshold: int) -> set:
    '''
    Count k-size candidate itemsets and returns those that are frequent

    Args:
        basket_rdd (list)
        candidates_list (list)
        threshold (int)
    Returns:
        
    '''
    candidates_count = defaultdict(int)
    for _, item_list in basket_rdd:
        for candidate in candidates_list:
            if set(candidate) <= set(item_list):
                candidates_count[candidate] += 1 # increase 1 count

    return {k for k,v in candidates_count.items() if v >= threshold}

def hash_items(basket_rdd: list) -> dict:
    '''Translates item names into integers from 1 to n

    Args:
        basket_rdd (list): [(key, value), ...]

    Returns:
        (dict): {item: hash, ...}
    '''
    subset_items = set()
    for _, list_items in basket_rdd:
        subset_items = subset_items | set(list_items)
    subset_items = list(subset_items)
    n = range(1, len(subset_items) + 1) # n is the number of distinct items
    map_to_int = { item: n_item for item, n_item in zip(subset_items, n)}
    return map_to_int


def a_priori_algorithm(partitionData: map, threshold: int, size_input_file: int) -> tuple:

    partitionData = list(partitionData)
    # Compute Threshold
    chunk_p_s_threshold = p_s_threshold(partitionData, threshold, size_input_file)

    # Hash partition data since items can be strings (higher performance)
    map_item_hash = hash_items(partitionData)
    inv_map_item_hash = {v: k for k, v in map_item_hash.items()} # Look-up table to convert back to original value
    user_basket_memory_hashed = []
    for key, list_items in partitionData:
        user_basket_memory_hashedlist = [map_item_hash[item] for item in list_items]
        user_basket_memory_key = key
        user_basket_memory_hashed.append((user_basket_memory_key, user_basket_memory_hashedlist))

    # A-Pripri Algorithm 
    k = 1
    C_k = [(item,) for item in map_item_hash.values()] # Singletons Candidates
    L_k_1 = count_itemsets_of_size_k(user_basket_memory_hashed, C_k, chunk_p_s_threshold) # all frequent singletons
    L_1 = L_k_1.copy()
    k += 1
    while len(L_k_1) > 0:
        for freq_item in L_k_1:  yield (tuple([inv_map_item_hash[item] for item in tuple(sorted(freq_item))]), 1) # Convert hash back to original name
        C_k = generate_candidates(L_k_1, L_1, k)
        L_k_1 = count_itemsets_of_size_k(user_basket_memory_hashed, C_k, chunk_p_s_threshold) # all frequent singletons
        k += 1


def p_s_threshold(partitionData: map, support_threshold: int, size_input_file: int) -> int:
    '''
    Lower the support threshold from s to ps if each Map task gets fraction p of the total input file
    
    Args:
        partitionData (map)
        support_threshold (int): s
        size_input_file (int)
    Returns:
        (int): p*s
    '''

    return round(support_threshold*(len(partitionData)/size_input_file))


def count_freq_itemsets(partitionData: map, candidate_itemsets: list) -> tuple:
    '''Count candidate itemset.The output is a set of key-value pairs (C, v),
    where C is one of the candidate sets and v is the support for that itemset among
    the baskets that were input to this Map task
    
    Args:
        partitionData (map)
    Returns:```````````````
        (tuple): (C,v)
    '''
    candidates_count = defaultdict(int)
    partitionData = list(partitionData)
    for candidate in candidate_itemsets:
        for _, item_list in partitionData:
            if set(candidate) <= set(item_list):
                candidates_count[candidate] += 1 # increase 1 count
        yield (candidate, candidates_count[candidate])


def save_to_file(file_path: str, header_candidate: str, list_of_candidates: list, header_frequent_items: str, list_of_freq_items: list) -> None:
    '''Save data to file
    Args:
        file_path (str)
        header_candidate (str)
        list_of_candidates (list)
        header_frequent_items (str)
        list_of_freq_items (list)
    '''
    with open(file_path, 'w') as fp:
        fp.write(f'{header_candidate}:\n')
        output_pairs = format_output_pairs(list_of_candidates)
        fp.write(output_pairs)
        fp.write(f'\n\n{header_frequent_items}:\n')
        output_pairs = format_output_pairs(list_of_freq_items)
        fp.write(output_pairs)


def format_output_pairs(list_of_items: list) -> str:
    '''Format list of tuple itemsets and convert them to string. Creates new line for each length of pair.

    Args:
        list_of_items (list): list of items
    Returns:
        (str):
    '''
    output_string = ''
    last_length_pair = len(list_of_items[0])
    for pair in list_of_items:
        tmp_string = "', '".join(list(pair))
        tmp_string = f"('{tmp_string}'),"
        if len(pair) != last_length_pair:
            last_length_pair = len(pair)
            output_string = output_string[:-1] + '\n' + '\n'
        output_string += tmp_string
    return output_string[:-1] # Remove last comma before \n



if __name__ == '__main__':
    # Read Arguments
    if len(sys.argv) != 5:
        print("Invalid Arguments")
        exit(1)

    case_number = int(sys.argv[1])
    support_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]

    # Start SparkContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR') 
    # Read File, skipping header
    start_time = time.time()
    review = sc.textFile(input_file_path).zipWithIndex().\
        filter(lambda x: x[1] > 0).\
            map(lambda line: line[0].split(","))

    # Select between cases
    # Case = 1 -> User basket
    # Case = 2 -> Business basket
    if case_number == 1:
        review = review.map(lambda x: (x[0], x[1]))
    elif case_number == 2:
        review = review.map(lambda x: (x[1], x[0]))
    else:
        print("Invalid case number")
        exit(1)

    basket_rdd = review.groupByKey().mapValues(set).\
        map(lambda x: (x[0], list(x[1])))
    
    #################
    # SON Algorithm #
    #################

    # Map Task 1
    size_input_file = basket_rdd.count()
    map_task_1 = basket_rdd.\
        mapPartitions(lambda partition: a_priori_algorithm(partition, support_threshold, size_input_file))
    
    # Reduce Task 1
    reduce_task_1 = map_task_1.map(lambda x: x[0]).distinct().sortBy(lambda x: (len(x), x))

    candidate_itemsets = reduce_task_1.collect()

    # Map Task 2
    map_task_2 = basket_rdd\
        .mapPartitions(lambda partition: count_freq_itemsets(partition, candidate_itemsets))
    
    # Reduce Task 2
    # The Reduce tasks take the itemsets they are given as keys and sum the associated values.
    # If candidate_itemset count > threshold return`` 
    reduce_task_2 = map_task_2.\
        reduceByKey(lambda x,y: x + y).filter(lambda x: x[1] >= support_threshold).\
            map(lambda x: x[0]).sortBy(lambda x: (len(x), x))
    
    frequent_itemset = reduce_task_2.collect()


    #############
    # Save File #
    #############
    save_to_file(output_file_path, 'Candidates', candidate_itemsets, 'Frequent Itemsets', frequent_itemset)
    end_time = time.time()
    print('Duration: ', end_time -  start_time)
