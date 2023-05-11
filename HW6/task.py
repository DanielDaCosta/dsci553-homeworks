from pyspark import SparkContext
from sklearn.cluster import KMeans, AffinityPropagation
from collections import defaultdict
import math
import copy
import sys
import time

## UPDATE CLUSTE STATS AFTER ASSIGNING POINTS

LARGE_K = 5

def compute_clusters(input_data: list, n_cluster: int, affinity_clustering=False, random_seed=42) -> list:

    if affinity_clustering:
        affinity = AffinityPropagation(random_state=random_seed)
        affinity.fit(input_data)
        labels = affinity.labels_

    else:
        # K-means
        k_means = KMeans(n_clusters=n_cluster, random_state=random_seed, n_init=10)

        k_means.fit(input_data)

        labels = k_means.labels_

    return labels


def divide_into_clusters(data_points: list, input_data: list, n_cluster: int, random_seed=42) -> list:
    '''Return key-value pair [(cluster_n, data_point)]
    '''

    # Compute clusters
    n_cluster_tmp = n_cluster
    affinity_clustering = False # Perform Affinity Propagation Clustering of data if True
    if len(input_data) < n_cluster_tmp:
        affinity_clustering=True
        # k_means_labels = DBSCAN(eps=3, min_samples=5).fit(cluster_data_chunk_features_values).labels_
    # else:
    # print(n_cluster_tmp)
    k_means_labels = compute_clusters(input_data, n_cluster=n_cluster_tmp, affinity_clustering=affinity_clustering)

    # print(data_points, k_means_labels)
    cluster_data_chunk_labels = []
    for i, (data_idx, cluster) in enumerate(zip(data_points, k_means_labels)):
        cluster_data_chunk_labels.append((cluster, data_idx))
    

    return cluster_data_chunk_labels


def generate_CS_DS_stats(set_clusters: set, data_features: dict) -> tuple:
    '''Generate CS or DS clusters statistcs.

    Args:
        set_clusters (list): {(cluster_id,  data_point), ...}

    Returns:
        set_cluster_data_points (dict): {cluster_id: [data_points], ...}
        set_clusters_N (dict): {cluster_id: N_points}
        set_clusters_SUM (dict): (cluster_id: [SUM_1, SUM_2...])
        set_clusters_SUMQ (dict): (cluster_id: [SUMQ_1, SUMQ_2...])
    '''
    set_clusters_rdd = sc.parallelize(set_clusters)

    set_cluster_data_points = set_clusters_rdd.groupByKey().mapValues(list).collectAsMap()
    set_clusters_N = set_clusters_rdd.map(lambda x: (x[0], 1)).\
        reduceByKey(lambda x, y: x+y).collectAsMap()
    set_clusters_features = set_clusters_rdd.map(lambda x: (x[0], data_features[x[1]]))

    set_clusters_SUM = set_clusters_features.\
        reduceByKey(lambda x, y: [x[i] + y[i] for i in range(len(x))]).collectAsMap()
    set_clusters_SUMQ = set_clusters_features.\
    mapValues(lambda x: [i**2 for i in x]).reduceByKey(lambda x, y: [x[i] + y[i] for i in range(len(x))]).collectAsMap()


    return set_cluster_data_points, set_clusters_N, set_clusters_SUM, set_clusters_SUMQ


def generate_RS_CS(data_points: list, input_data: list, n_cluster: int, random_seed=42) -> list:

    
    cluster_data_chunk_labels = divide_into_clusters(data_points, input_data, n_cluster)
    # print(cluster_data_chunk_labels)
    cluster_data_chunk_labels = sc.parallelize(cluster_data_chunk_labels)
    cluster_data_chunk_labels_per_cluster = cluster_data_chunk_labels.groupByKey().mapValues(list)

    # Generate Retained Set (RS)

    RS = set()
    # Move all the clusters that contain only one point to RS
    outliers_rdd = cluster_data_chunk_labels_per_cluster.filter(lambda x: len(x[1]) == 1)#.flatMap(lambda x: x[1])
    outliers = outliers_rdd.map(lambda x: x[1][0]).collect()
    # print(outliers)

    RS.update(outliers)

    # Generate Compression set  CS
    not_outliers_rdd = cluster_data_chunk_labels_per_cluster.filter(lambda x: len(x[1]) != 1).flatMap(lambda x: [(x[0], v) for v in x[1]])\
    #map(lambda x: (x, cluster_data_chunk_features_dict[x])).collectAsMap()
    CS = set()
    not_outliers = not_outliers_rdd.collect()
    # print(not_outliers)
    CS.update(not_outliers)

    return RS, CS


def compute_mahalanobis_distance(cluster: int, data_point_features: list, cluster_N: int,
                                 cluster_SUM: list, cluster_SUMQ: list) -> float:

    N = cluster_N
    cum_sum = 0 
    for i, x_i in enumerate(data_point_features):
        c_i = cluster_SUM[i]/N 
        std_i = math.sqrt(cluster_SUMQ[i]/N - \
            (cluster_SUM[i]/N)**2)
        # if std_i == 0:
        #     print(cluster_SUMQ[i]/N)
        #     print((cluster_SUM[i]/N)**2)
        #     print(N)
        #     print(data_point_features)
        #     print(cluster)
        # print(std_i)
        tmp = ((x_i - c_i)/std_i)**2
        cum_sum += tmp
    return math.sqrt(cum_sum)


def get_closest_cluster(data_point_features: list, threshold: float,
    set_clusters_stats: list) -> int:
    
    dist_list = {}
    for cluster, cluster_stats in enumerate(set_clusters_stats):
        # cluster_stats => (data_points, N, SUM, SUMQ)
        distance = compute_mahalanobis_distance(cluster, data_point_features, cluster_stats[1], cluster_stats[2], cluster_stats[3])
        if distance < threshold:
            dist_list[cluster] = distance

    assigned_cluster = None
    
    if dist_list:
        # print(dist_list)
        assigned_cluster = min(dist_list, key=lambda k: dist_list[k])
    return assigned_cluster


def update_clusters_statistics(
    cluster, data_point: int, data_point_features: list, set_clusters_stats: list):
    '''Update cluster statistics.
    SUM, SUMQ, N and add data point to cluster_list
    Args:
        set_clusters_stats (list): (points, N, SUM, SUMQ)
    '''

    set_clusters_stats[cluster][0].append(data_point)
    set_clusters_stats[cluster][1] += 1 # N

    for dim, x_i in enumerate(data_point_features):
        set_clusters_stats[cluster][2][dim] += x_i # SUM
        set_clusters_stats[cluster][3][dim] += x_i**2 # SUMQ


def merge_cluters_stats(cluster_1_stats: list, cluster_2_stats: list) -> list:
    '''Merge cluster statistics:
    - combine list of data_points
    - N = N1 + N2
    - SUM = SUM1 + SUM2
    - SUMQ = SUMQ1 + SUMQ2
    '''
    
    # Sum N
    new_cluster = []

    # Combine data points
    new_cluster.append(cluster_1_stats[0] + cluster_2_stats[0]) 
    # Add N
    new_cluster.append(cluster_1_stats[1] + cluster_2_stats[1])
    # Add SUM
    new_cluster.append(list(map(lambda x, y: x + y, cluster_1_stats[2], cluster_2_stats[2])))
    # Add SUMQ
    new_cluster.append(list(map(lambda x, y: x + y, cluster_1_stats[3], cluster_2_stats[3])))

    return new_cluster # (data_points, N, SUM, SUMQ)


def merge_cs_ds_clusters(cs_clusters_stats: list, ds_clusters_stats: list, threshold: float) -> tuple:

    cs_clusters_stats_tmp = copy.deepcopy(cs_clusters_stats)
    ds_clusters_stats_tmp = copy.deepcopy(ds_clusters_stats)

    list_of_merged_clusters = []


    def find_closest_clusters():
        min_distance = threshold
        min_cluster = None
        idx_pairs = [(i,j) for i in range(len(cs_clusters_stats_tmp)) for j in range(i, len(ds_clusters_stats_tmp))]
        for (c_i, d_j) in idx_pairs:
            # treat c_i as a data point
            center_c_i = list(map(lambda x: x / cs_clusters_stats_tmp[c_i][1], cs_clusters_stats_tmp[c_i][2]))
            distance_i = compute_mahalanobis_distance(c_i, center_c_i, ds_clusters_stats_tmp[d_j][1], ds_clusters_stats_tmp[d_j][2], ds_clusters_stats_tmp[d_j][3])
            # treat d_j as a data point
            center_d_j = list(map(lambda x: x / ds_clusters_stats_tmp[d_j][1], ds_clusters_stats_tmp[d_j][2]))
            distance_j = compute_mahalanobis_distance(d_j, center_d_j, cs_clusters_stats_tmp[c_i][1], cs_clusters_stats_tmp[c_i][2], cs_clusters_stats_tmp[c_i][3])
            # Avg distance
            distance = (distance_i + distance_j)/2
            if distance < min_distance:
                min_distance = distance
                min_cluster = (c_i, d_j)
        return min_cluster, min_distance

    min_cluster, min_distance = find_closest_clusters()
    while min_distance < threshold:
        # Merge cluster c_i with c_j
        c_i, d_j = min_cluster
        list_of_merged_clusters.append((c_i, d_j))
        new_merged_cluster = merge_cluters_stats(cs_clusters_stats_tmp[c_i], ds_clusters_stats_tmp[d_j])
        ds_clusters_stats_tmp[d_j] = new_merged_cluster # replace c_i with new cluster
        cs_clusters_stats_tmp.pop(c_i) # remove c_j
        min_cluster, min_distance = find_closest_clusters()

    # if not min_cluster: 
    return cs_clusters_stats_tmp, ds_clusters_stats_tmp, list_of_merged_clusters
    # else: # if cs_clusters_stats_tmp is empty
    # return None, None, list_of_merged_clusters
    

def merge_cs_clusters(cs_clusters_stats: list, threshold: float) -> tuple:
    '''Merge CS clusters
    '''

    cs_clusters_stats_tmp = copy.deepcopy(cs_clusters_stats)
    list_of_merged_clusters = []


    def find_closest_clusters():
        min_distance = threshold
        min_cluster = None
        idx_pairs = [(i,j) for i in range(len(cs_clusters_stats_tmp)) for j in range(i+1, len(cs_clusters_stats_tmp))]
        for (c_i, c_j) in idx_pairs:
            # treat c_i as data point
            center_c_i = list(map(lambda x: x / cs_clusters_stats_tmp[c_i][1], cs_clusters_stats_tmp[c_i][2]))
            distance_i = compute_mahalanobis_distance(c_i, center_c_i, cs_clusters_stats_tmp[c_j][1], cs_clusters_stats_tmp[c_j][2], cs_clusters_stats_tmp[c_j][3])
            center_c_j = list(map(lambda x: x / cs_clusters_stats_tmp[c_j][1], cs_clusters_stats_tmp[c_j][2]))
            distance_j = compute_mahalanobis_distance(c_j, center_c_j, cs_clusters_stats_tmp[c_i][1], cs_clusters_stats_tmp[c_i][2], cs_clusters_stats_tmp[c_i][3])
            distance = (distance_i + distance_j)/2
            if distance < min_distance:
                min_distance = distance
                min_cluster = (c_i, c_j)
        return min_cluster, min_distance

    min_cluster, min_distance = find_closest_clusters()
    while min_distance < threshold:
        # Merge cluster c_i with c_j
        c_i, c_j = min_cluster
        list_of_merged_clusters.append((c_i, c_j))
        new_merged_cluster = merge_cluters_stats(cs_clusters_stats_tmp[c_i], cs_clusters_stats_tmp[c_j])
        cs_clusters_stats_tmp[c_j] = new_merged_cluster # replace c_j with new cluster
        cs_clusters_stats_tmp.pop(c_i) # remove c_i
        min_cluster, min_distance = find_closest_clusters()

    # if min_cluster: 
    return cs_clusters_stats_tmp, list_of_merged_clusters
    # else: # if cs_clusters_stats_tmp is empty
        # return [], list_of_merged_clusters


def save_output(rows: list, output_path: str, write_or_append: str):
    with open(output_path, write_or_append) as f:
        f.write( '\n'.join(', '.join(f"{value}" for value in key_value) for key_value in rows))
        f.write('\n')


if __name__ == '__main__':
    # Read Arguments
    if len(sys.argv) != 4:
        print("Invalid Arguments")
        exit(1)
    input_file_path = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file_path = sys.argv[3]

    # Start SparkContext
    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR')


    random_seed = 42
    # read data in chunks of 20%
    cluster_data = sc.textFile(input_file_path).\
        map(lambda line: line.split(",")).randomSplit([.20, .20, .20, .20, .20], random_seed)

    ##########
    # Step 1 #
    ##########
    cluster_data_chunk = cluster_data[0]
    # Filtering only the features
    # Notice that the number of the dimensions could be different from the hw6_clustering.txt
    cluster_data_chunk_features = cluster_data_chunk.map(lambda x: (int(x[0]), list(map(float, x[2:]))))
    cluster_data_chunk_features_dict = cluster_data_chunk_features.collectAsMap()

    cluster_data_chunk_features_values = [list(v) for v in cluster_data_chunk_features_dict.values()]

    ##########
    # Step 2 #
    ##########
    cluster_data_chunk_labels = divide_into_clusters(
        cluster_data_chunk_features_dict.keys(),
        cluster_data_chunk_features_values, LARGE_K*n_cluster)
    cluster_data_chunk_labels = sc.parallelize(cluster_data_chunk_labels)
    
    ##########
    # Step 3 #
    ##########
    # Generate Retained Set
    RS = set()
    # Move all the clusters that contain only one point to RS
    cluster_data_chunk_labels_per_cluster = cluster_data_chunk_labels.groupByKey().mapValues(list)
    outliers_rdd = cluster_data_chunk_labels_per_cluster.filter(lambda x: len(x[1]) == 1).flatMap(lambda x: x[1])
    outliers = outliers_rdd.collect()
    RS.update(outliers)

    ##########
    # Step 4 #
    ##########
    not_outliers_dict = cluster_data_chunk_labels_per_cluster.filter(lambda x: len(x[1]) != 1).flatMap(lambda x: x[1]).\
    map(lambda x: (x, cluster_data_chunk_features_dict[x])).collectAsMap()
    not_outliers_features = [list(v) for v in not_outliers_dict.values()]
    ds_clusters = divide_into_clusters(not_outliers_dict.keys(), not_outliers_features, n_cluster)

    ##########
    # Step 5 #
    ##########
    # Generate Discard set
    ds_clusters_points, ds_clusters_N, ds_clusters_SUM, ds_clusters_SUMQ = generate_CS_DS_stats(ds_clusters, cluster_data_chunk_features_dict)
    ds_clusters_stats = [] # (points, N, SUM, SUMQ)

    for ds_cluster in ds_clusters_N.keys():
        ds_clusters_stats.append(
            [ds_clusters_points[ds_cluster], ds_clusters_N[ds_cluster], ds_clusters_SUM[ds_cluster], ds_clusters_SUMQ[ds_cluster]])

    ##########
    # Step 6 #
    ##########
    RS_features = {outlier_point: cluster_data_chunk_features_dict[outlier_point] for outlier_point in RS}
    RS, cs_clusters = generate_RS_CS(RS, list(RS_features.values()), LARGE_K*n_cluster)
    cs_clusters_points, cs_clusters_N, cs_clusters_SUM, cs_clusters_SUMQ = generate_CS_DS_stats(cs_clusters, RS_features)
    cs_clusters_stats = [] # (data_points, N, SUM, SUMQ)

    for cs_cluster in cs_clusters_N.keys():
        cs_clusters_stats.append(
            [cs_clusters_points[cs_cluster], cs_clusters_N[cs_cluster], cs_clusters_SUM[cs_cluster], cs_clusters_SUMQ[cs_cluster]])
        

    # Save output Round 1
    # “the number of the discard points”,
    # “the number of the clusters in the compression set”,
    # “the number of the compression points”, and “the number of the points in the retained set”
    row = [[f'The intermediate results:']]
    save_output(row, output_file_path, 'w')
    round_i = 1
    ds_points = sum([cluster[1] for cluster in ds_clusters_stats])
    n_cs_clusters = len(cs_clusters_stats)
    cs_points = sum([cluster[1] for cluster in cs_clusters_stats])
    n_rs_points = len(RS)
    row = [[f'Round {round_i}:', ds_points, n_cs_clusters, cs_points, n_rs_points]]
    save_output(row, output_file_path, 'a')
        
    d = len(cluster_data_chunk_features_values[0])
    threshold = 2*math.sqrt(d)
    for cluster_data_chunk in cluster_data[1:]: # cluster_data[0] already used in Initialization phase

        ##########
        # Step 7 #
        ##########
        # Filtering only the features
        # Notice that the number of the dimensions could be different from the hw6_clustering.txt
        cluster_data_chunk_features = cluster_data_chunk.map(lambda x: (int(x[0]), list(map(float, x[2:]))))
        cluster_data_chunk_features_dict = cluster_data_chunk_features.collectAsMap()

        DS_updates = {} # store points that will be added to DS
        CS_updates = {}# store points that will be added to CS
        for data_point, data_point_features in cluster_data_chunk_features_dict.items():

            ##########
            # Step 8 #
            ##########
            assigned_cluster = get_closest_cluster(data_point_features, threshold, ds_clusters_stats)
            if assigned_cluster: # add to DS cluster
                DS_updates[data_point] = (assigned_cluster, data_point_features)
                # update_clusters_statistics(assigned_cluster, data_point, data_point_features, ds_clusters_stats)

            else: 
            ##########
            # Step 9 #
            ##########
            # For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and
            #  assign the points to the nearest CS cluster
            # if not assigned_cluster:
                assigned_cluster = get_closest_cluster(data_point_features, threshold, cs_clusters_stats)
                if assigned_cluster:
                    CS_updates[data_point] = (assigned_cluster, data_point_features)
                    # update_clusters_statistics(assigned_cluster, data_point, data_point_features, cs_clusters_stats)

            ###########
            # Step 10 #
            ###########
            
            # For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS
            if not assigned_cluster:
                RS.add(data_point)
                RS_features[data_point] = data_point_features
        # Update DS and CS Clusters
        for data_point_update, (assigned_cluster, data_point_features)  in DS_updates.items():
            update_clusters_statistics(assigned_cluster, data_point_update, data_point_features, ds_clusters_stats)
        for data_point_update, (assigned_cluster, data_point_features)  in CS_updates.items():
            update_clusters_statistics(assigned_cluster, data_point_update, data_point_features, cs_clusters_stats)

        # Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters)
        # to generate CS (clusters with more than one points) and RS (clusters with only one point).

        # print(RS)

        ###########
        # Step 11 #
        ###########
        RS_features = {outlier_point: outlier_point_data for outlier_point, outlier_point_data in RS_features.items() if outlier_point in RS}
        if RS: # RS not empty
            RS, cs_clusters = generate_RS_CS(RS, list(RS_features.values()), LARGE_K*n_cluster)

            cs_clusters_points, cs_clusters_N, cs_clusters_SUM, cs_clusters_SUMQ = generate_CS_DS_stats(cs_clusters, RS_features)

            # append new_clusters to cs_clusters_stats (lists)
            for cs_cluster in cs_clusters_N.keys():
                cs_clusters_stats.append(
                    [cs_clusters_points[cs_cluster], cs_clusters_N[cs_cluster], cs_clusters_SUM[cs_cluster], cs_clusters_SUMQ[cs_cluster]])
            
        ###########
        # Step 12 #
        ###########
        # Merge CS Clusters
        cs_clusters_stats, _ = merge_cs_clusters(cs_clusters_stats, threshold)

        # Save output Round (round_i + 1)
        # “the number of the discard points”,
        # “the number of the clusters in the compression set”,
        # “the number of the compression points”, and “the number of the points in the retained set”

        if round_i != 4:
            ds_points = sum([cluster[1] for cluster in ds_clusters_stats])
            n_cs_clusters = len(cs_clusters_stats)
            cs_points = sum([cluster[1] for cluster in cs_clusters_stats])
            n_rs_points = len(RS)
            round_i += 1
            row = [[f'Round {round_i}:', ds_points, n_cs_clusters, cs_points, n_rs_points]]
            save_output(row, output_file_path, 'a')

        # cs_clusters_stats = merge_cs_clusters(cs_clusters_stats, threshold)
        # print(cs_clusters_stats)
        # cs_clusters_rdd = sc.parallelize(CS)
        # cs_clusters_N = cs_clusters_rdd.map(lambda x: (x[0], 1)).\
        #     reduceByKey(lambda x, y: x+y).collectAsMap()
        # cs_clusters_features = cs_clusters_rdd.map(lambda x: (x[0], cluster_data_chunk_features_dict[x[1]]))

        # cs_clusters_SUM = cs_clusters_features.\
        #     reduceByKey(lambda x, y: [x[i] + y[i] for i in range(len(x))]).collectAsMap()
        
        # cs_clusters_SUMQ = cs_clusters_features.\
        #     mapValues(lambda x: [i**2 for i in x]).reduceByKey(lambda x, y: [x[i] + y[i] for i in range(len(x))]).collectAsMap()

    ###########
    # Step 13 #
    ###########
    # Merge CS with DS clusters
    cs_clusters_stats, ds_clusters_stats, _ = merge_cs_ds_clusters(cs_clusters_stats, ds_clusters_stats, threshold)
    ds_points = sum([cluster[1] for cluster in ds_clusters_stats])
    n_cs_clusters = len(cs_clusters_stats)
    cs_points = sum([cluster[1] for cluster in cs_clusters_stats])
    n_rs_points = len(RS)
    round_i += 1
    row = [[f'Round {round_i}:', ds_points, n_cs_clusters, cs_points, n_rs_points]]
    save_output(row, output_file_path, 'a')

    # Save clustering results
    # “data points index”,
    # “clusters”
    ds_points = {}
    for cluster_id, cluster in enumerate(ds_clusters_stats):
        for data_point in cluster[0]:
            ds_points[data_point] = cluster_id

    outliers = {}

    for data_point in RS:
        outliers[data_point] = -1
    for cluster_id, cluster in enumerate(cs_clusters_stats):
        for data_point in cluster[0]:
            outliers[data_point] = -1
    row = [['\nThe clustering results:']]
    save_output(row, output_file_path, 'a')
    final_output = [[data_point, cluster_id] for (data_point, cluster_id) in sorted({**ds_points, **outliers}.items())]
    save_output(final_output, output_file_path, 'a')

    end_time = time.time()
    print('Duration: ', end_time - start_time)
