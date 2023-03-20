from pyspark import SparkContext
import time
import json
import sys
import random
import math
import itertools
import csv

# Best
N_CO_RATED_USERS = 0 # Minimum number of users that need to have co-rated both items
N_NEIGHBORS_ITEMS = 18 # number of neighbors for item-based collaborative filtering
AVG_USER_TRAINING_SET = 3.766098

# N_CO_RATED_USERS = 1 # Minimum number of users that need to have co-rated both items
# N_NEIGHBORS_ITEMS = 6 # number of neighbors for item-based collaborative filtering

def map_validation_items(item_hash: str,
                         user_hash: str,
                         index_items_train:
                         dict, index_users_train: dict) -> tuple:
    '''Convert user and item hash into idx. If user or item not found return -1'''
    try:
        item_idx = index_items_train[item_hash]
    except:
        item_idx = -1
    try: 
        user_idx = index_users_train[user_hash]
    except:
        user_idx = -1

    return (item_idx, user_idx)

def save_csv(rows: list, output_file_name: str, header=['user_id', 'business_id', 'prediction']):
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([header])
        writer.writerows(rows)

if __name__ == '__main__':

    # Read Arguments
    if len(sys.argv) != 4:
        print("Invalid Arguments")
        exit(1)
    train_file_name = sys.argv[1]
    test_file_name = sys.argv[2]
    output_file_name = sys.argv[3]

    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR') 

    # Read File, skipping header
    review_train = sc.textFile(train_file_name).zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(","))
    review_val = sc.textFile(test_file_name).zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(","))


    # Map items and users to idx
    items_train = review_train.map(lambda x: x[1]).distinct().zipWithIndex()
    users_train = review_train.map(lambda x: x[0]).distinct().zipWithIndex()
    index_items_train = items_train.map(lambda x: (x[0], x[1])).collectAsMap()
    index_users_train = users_train.map(lambda x: (x[0], x[1])).collectAsMap()


    # Format dataset to ((business_idx, user_idx), rating)
    review_train_idx = review_train.map(lambda x: ((index_items_train[x[1]], index_users_train[x[0]]), float(x[2])))


    # (Items, (user, ratings)
    items_user_ratings = review_train_idx.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(dict).collectAsMap()
    items_user_ratings_broadcast = sc.broadcast(items_user_ratings)

    # Mapping Validation business and users id to idx
    review_val_idx = review_val.map(lambda x: (map_validation_items(x[1], x[0], index_items_train, index_users_train), (x[1], x[0])))
    # items_train_pairs = review_train_idx.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().\
    #         mapValues(lambda x: len(x)).filter(lambda x: x[1] >= N_CO_RATED_USERS).map(lambda x: x[0]).sortBy(lambda x: x).collect()

    # Target and Users Idx
    # target_items_idx = review_val_idx.map(lambda x: x[0][0]).distinct()
    target_items_idx = review_val_idx.map(lambda x: x[0][0]).distinct()
    target_users_idx = review_val_idx.map(lambda x: x[0][1]).distinct().map(lambda x: (x, None))

    # Filter train items based on validation. Reducing computation to only items that are present in validation set.
    # index_items_train_val = sorted(target_items_idx.filter(lambda x: x != -1).collect())

    # Generate items-pairs

    # num_pairs = items_train_pairs.count()

    # item_pairs = sc.parallelize([i for i in range(num_pairs)]).flatMap(lambda x: [(x, i) for i in index_items_train_val if i > x])
    # item_pairs = sc.parallelize([i for i in index_items_train_val]).flatMap(lambda x: [(i, x) for i in items_train_pairs if i < x])

    # Filter items that have co-rated users >= N_CO_RATED_USERS

    def filter_item_pairs(item_1_idx: int, item_2_idx: int, threshold: int) -> bool:
        '''Filter item pairs that who have been co-rated by N users (where N >= threshold)

        Args:
            item_1_idx (int)
            item_2_idx (int)
            items_user_ratings_broadcast (sc.broadcast) => global variable
            threshold (int)
        
        Return:
            (bool) True or False
        '''
        co_rated_users = set(items_user_ratings_broadcast.value[item_1_idx].keys()).\
            intersection(set(items_user_ratings_broadcast.value[item_2_idx].keys()))
        return len(co_rated_users) >= threshold

    # item_pairs_filtered = item_pairs.filter(lambda x: filter_item_pairs(x[0], x[1], N_CO_RATED_USERS))

    # Target user ratings
    target_user_ratings = target_users_idx.leftOuterJoin(review_train_idx.map(lambda x: (x[0][1], (x[0][0], x[1])))).map(lambda x: (x[0], x[1][1])).groupByKey().mapValues(list)
    target_user_ratings = sc.broadcast(target_user_ratings.map(lambda x: (x[0], dict(x[1]) if x[1][0] is not None else (x[0], {}))).collectAsMap())

    # Compute the items ratings mean
    items_mean_ratings = review_train_idx.map(lambda x: (x[0][0], x[1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collectAsMap()
    items_mean_ratings = sc.broadcast(items_mean_ratings)
    # Compute the user ratings mean
    user_mean_ratings = review_train_idx.map(lambda x: (x[0][1], x[1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collectAsMap()
    user_mean_ratings = sc.broadcast(user_mean_ratings)

    def compute_items_avg(user_ratings_1: dict, user_ratings_2: dict) -> tuple:
        avg_user_1 = sum(user_ratings_1.values())/len(user_ratings_1)
        avg_user_2 = sum(user_ratings_2.values())/len(user_ratings_2)
        return avg_user_1, avg_user_2


    def pearson_similarity_w(item_1_idx: int, item_2_idx: int) -> float:
        '''Compute pearson similarity for co-rated items only.
        

        Args:
            item_1_ratings (dict): {'U1': 2, 'U2': 3, 'U4': 5}
            item_2_ratings (dict): {'U1': 1, 'U3': 4, 'U4': 3}
            global items_user_ratings_broadcast

        Returns:
            (float) pearson similarity
        '''

        user_ratings_1 = items_user_ratings_broadcast.value[item_1_idx]
        user_ratings_2 = items_user_ratings_broadcast.value[item_2_idx]
        similar_keys = set(user_ratings_1.keys()) & set(user_ratings_2.keys())

        # if similar_keys:
        #     filtered_user_ratings_1 = {k: v for k, v in user_ratings_1.items() if k in similar_keys}
        #     filtered_user_ratings_2 = {k: v for k, v in user_ratings_2.items() if k in similar_keys}

        #     item_1_mean_rating, item_2_mean_rating = compute_items_avg(filtered_user_ratings_1, filtered_user_ratings_2)
        item_1_mean_rating = items_mean_ratings.value[item_1_idx]
        item_2_mean_rating = items_mean_ratings.value[item_2_idx]

        numerator = 0
        denominator_user_1 = 0
        denominator_user_2 = 0

        # Edge Cases
        if len(similar_keys) == 0 or len(similar_keys) == 1:
            absDiff = abs(item_1_mean_rating - item_2_mean_rating)
            if (absDiff <= 1):
                pearson_similarity = 1
            elif (absDiff <= 2):
                pearson_similarity = 0.5
            else:
                pearson_similarity = 0
            return pearson_similarity

        for user in similar_keys:
            # Numerator
            # (r_u_i - r_i_mean)*(r_u_j - r_j_mean)
            cov_user_1 = user_ratings_1[user] - item_1_mean_rating
            cov_user_2 = user_ratings_2[user] - item_2_mean_rating
            numerator += cov_user_1*cov_user_2

            # Denominator
            std_user_1 = (user_ratings_1[user] - item_1_mean_rating)**2
            std_user_2 = (user_ratings_2[user] - item_2_mean_rating)**2
            denominator_user_1 += std_user_1
            denominator_user_2 += std_user_2

        if numerator == 0:
            pearson_similarity = 0
        else:
            pearson_similarity = numerator/((denominator_user_1)**(1/2)*(denominator_user_2)**(1/2))
        return pearson_similarity
    
    # pearson_similarity_scores = item_pairs_filtered.map(lambda x:
    #     ((x[0], x[1]), pearson_similarity_w(x[0], x[1]))).filter(lambda x: x[1] > 0).collectAsMap()
    # pearson_similarity_scores = sc.broadcast(pearson_similarity_scores)

    def make_prediction(
            target_item: int,
            target_user: int):
        ''' Prediction for target_item
        
        Args:
            global target_user_ratings
            global pearson_similarity_scores
        '''

        unkwon_token = -1 # token for user or items that are not in training set
        if target_item == unkwon_token and target_user == unkwon_token: # new item and new user
            # print('New Item & New User')
            rating_pred = AVG_USER_TRAINING_SET
        elif target_item == unkwon_token: # new item
            # print('New Item')
            # print(target_item, target_user)
            # rating_pred = 3.823989
            rating_pred = user_mean_ratings.value[target_user]
        elif target_user == unkwon_token: # new user
            # print('New User')
            # rating_pred = 3.766098
            rating_pred = items_mean_ratings.value[target_item]
        else:
            
                numerator = 0
                denominator = 0
                items_rated_by_target_user = target_user_ratings.value[target_user]
                neighbors_item = []
                for item, rating in items_rated_by_target_user.items():
                    if filter_item_pairs(target_item, item, N_CO_RATED_USERS): # if item was co-rated by >= N_CO_RATED_USERS
                        w = pearson_similarity_w(target_item, item)
                        if w > 0:
                            neighbors_item.append((w, rating))                    
                if len(neighbors_item) == 0: # if no similar item is found
                    rating_pred = AVG_USER_TRAINING_SET
                    # rating_pred = 3.8
                    # rating_pred = items_mean_ratings.value[target_item]
                else:
                    # Retrieve top N_NEIGHBORS_ITEMS items
                    neighbors_item = sorted(neighbors_item, key=lambda x: -x[0])[:N_NEIGHBORS_ITEMS]
                    # print('AQUI')
                    for w, rating in neighbors_item:
                        numerator += rating*w
                        # sum(|w_i_n|)
                        denominator += abs(w)
                    
                    if numerator == 0:
                        rating_pred = AVG_USER_TRAINING_SET # it will never happen
                    else: 
                        rating_pred = numerator/denominator
        return rating_pred
    
    result = review_val_idx.map(lambda x: (x[1][1], x[1][0], make_prediction(x[0][0], x[0][1])))
    save_csv(result.collect(), output_file_name)
    end_time = time.time()
    print('Duration: ', end_time - start_time)
