from pyspark import SparkContext
import time
import itertools
# from pyspark.sql import SparkSession
import os
import sys
from collections import defaultdict

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

# Node in Graph
class BFS_Node():
    def __init__(self, name):
        self.name = name
        self.level = 0
        self.parents = set() # {parents}
        # self.children = set() # {children}
        self.n_path = 0 # number of shortest path
        self.credit = 0 # each node other than the root node is given a credit of 1

    def __repr__(self):
        return f"[name: {self.name}, level: {self.level}, parent: {self.parents}, n_path: {self.n_path}, credit: {self.credit}]"
    

def BFS_node(root_node_name: BFS_Node, nodes_neighbors: dict):

    queue = [[root_node_name]]
    visited_nodes = [] # keep track of visited nodes
    graph = {} # BFS tree
    while queue:
        current_path = queue.pop(0)
        current_node = current_path[-1] # get last node of path. A->B->C
            # print(current_path)
        neighbours = nodes_neighbors[current_node]
        # if current_node == 'E':
            # print(neighbours)
        for neighbour in neighbours:
            if neighbour not in visited_nodes:
                new_path = list(current_path) 
                new_path.append(neighbour)
                # print(new_path)
                # if current_node == 'E':
                    # print(new_path)
                queue.append(new_path)

        # Add Node Class
        if current_node not in visited_nodes:
            curr_node = BFS_Node(current_node)
            curr_node.level = len(current_path) - 1
            curr_node.n_path += 1
            if curr_node.level > 0: # not root
                curr_node.parents.add(current_path[-2])
                curr_node.credit = 1
            
            graph[current_node] = curr_node

            visited_nodes.append(current_node) # add current_node
        else:
            # print()
            level = len(current_path) - 1
            # Only add parent if it's the shortest path
            if level == graph[current_node].level: # shortest path
                graph[current_node].parents.add(current_path[-2])
                graph[current_node].n_path += 1
             
    return graph


def girvan_newman_algorithm(nodes_neighbors: dict) -> dict:
    edges_betweenness = defaultdict(float)
    for target_node in nodes_neighbors.keys():
        # if target_node != 'F':
        #     continue
        bfs_target_node = BFS_node(target_node, nodes_neighbors)
        max_height = max([node.level for node in bfs_target_node.values()])
        for height in range(max_height, 0, -1):
            # get nodes at this level
            node_at_level = {
                node_key: node for node_key, node in bfs_target_node.items() if node.level == height
            }
            # print(node_at_level)
            for n_key, n_value in node_at_level.items():
                # print(n_value.n_path)
                for parent in n_value.parents:
                    bfs_target_node[parent].credit += n_value.credit*(bfs_target_node[parent].n_path/n_value.n_path)
                    edges_betweenness[frozenset((n_key, parent))] += (bfs_target_node[parent].n_path/n_value.n_path)*n_value.credit
                    # print(edges_betweenness[frozenset((n_key, parent))])
            # break

        # print(target_node)
        # break
    for edge in edges_betweenness: # Divide by 2 to get true betweenness. Since every shortest path will be counted twice (up and down), once for each of its endpoints
        edges_betweenness[edge] /= 2
        # edges_betweenness[edge] = round(edges_betweenness[edge], 5) # round the betweenness value to five digits after the decimal point
    return edges_betweenness


def save_output_betweenness(rows: dict, output_path: str, index_users_invert: dict):
    
    with open(output_path, 'w') as f:
        rows = {(index_users_invert[tuple(key)[0]], index_users_invert[tuple(key)[1]]): value for key, value in rows.items()} # frozenset to tuple

        f.write('\n'.join(f"('{key_tuple[0]}', '{key_tuple[1]}'),{round(value, 5)}" for key_tuple, value in sorted(rows.items(), key= lambda x: (-x[1], x[0]))))
                                                                                                                                                                           

def find_sub_graphs(nodes_neighbors: dict) -> list:
    '''Find sub-graphs. Example:
    nodes_neighbors = {'A': ['B', 'D'], 'B': ['A', 'C'], 'C': ['B'], 'D': ['A'], 'E': ['F'], 'F': ['E']}
    returns: [['A', 'B', 'D', 'C'], ['E', 'F']]

    Args:
        nodes_neighbors (dict)
    Returns:
        list
    '''

    visited = set()
    communities = []
    for node, neighbors in nodes_neighbors.items():
        if node not in visited:
            sub_graph = BFS_node(node, nodes_neighbors)
            sub_graph = list(sub_graph.keys())
            communities.append(frozenset(sub_graph))
            visited = visited.union(sub_graph)
            # print(visited)
    return communities


def compute_modularity(sub_graphs: list, nodes_neighbors: dict, size_m: int, nodes_degree: dict):

    cum_sum = 0
    for community in sub_graphs:
        for i in community:
            k_i = nodes_degree[i]
            for j in community:
                k_j = nodes_degree[j]
                if j in nodes_neighbors[i]: # check if edge between i and j exists
                    A_i_j = 1
                else:
                    A_i_j = 0
                cum_sum += (A_i_j - k_i*k_j/(2*size_m))
    return cum_sum/(2*size_m)


def find_communities(edges: dict, nodes_neighbors: dict, nodes_degree: dict) -> list:

    edges_filtered = edges.copy()
    m = len(edges_filtered) # number of edges
    nodes_neighbors_filtered = nodes_neighbors.copy()
    max_modularity = -1
    while len(edges_filtered) > 0: # remove edges with the highest betweenness
        max_betweenness = max(edges_filtered.values())
        max_keys = [key for key, value in edges_filtered.items() if value == max_betweenness] # keys to be removed. if two or more edges have the same (highest) betweenness, you should remove all those edges.
        for key in max_keys:
        #     if key[0] in nodes_neighbors[key[1]]:
            key_tmp = tuple(key)
            nodes_neighbors_filtered[key_tmp[0]].remove(key_tmp[1]) # remove both directions
            nodes_neighbors_filtered[key_tmp[1]].remove(key_tmp[0])
            # edges_filtered.pop(key)
        # Re-compute betwenness
        edges_filtered = girvan_newman_algorithm(nodes_neighbors_filtered) 

        # Generate graph
        sub_graphs = find_sub_graphs(nodes_neighbors_filtered)
        # Compute Modularity
        modularity = compute_modularity(sub_graphs, nodes_neighbors_filtered, m, nodes_degree)
        if modularity >= max_modularity:
            max_communities = sub_graphs
            max_modularity = modularity

    return max_communities


def save_output_communities(rows: list, output_path: str, index_users_invert: dict):
    output = []
    for row in rows:
        output.append(sorted([index_users_invert[i] for i in row]))
    with open(output_path, 'w') as f:
        output = sorted(output, key=lambda x: (len(x), x[0]))
        # return output
        f.write( '\n'.join(', '.join(f"'{value}'" for value in key_value) for key_value in output))
        f.write('\n')


if __name__ == '__main__':

    # Read Arguments
    if len(sys.argv) != 5:
        print("Invalid Arguments")
        exit(1)
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    betweenness_output_file_path = sys.argv[3]
    community_output_file_path = sys.argv[4]

    # Start SparkContext
    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR')

    # Read File, skipping header
    review = sc.textFile(input_file_path).zipWithIndex().filter(lambda x: x[1] > 0).\
        map(lambda line: line[0].split(","))
    
    # Map business and users to idx
    business_idx = review.map(lambda x: x[1]).distinct().zipWithIndex()
    users_idx = review.map(lambda x: x[0]).distinct().zipWithIndex()
    index_business = business_idx.map(lambda x: (x[0], x[1])).sortByKey().collectAsMap() # convert to indexes in lexicographical order
    index_users = users_idx.map(lambda x: (x[0], x[1])).sortByKey().collectAsMap()
    index_users_invert = {value: key for key, value in index_users.items()}


    # Format dataset to (g)
    review_idx = review.map(lambda x: (index_users[x[0]], index_business[x[1]]))

    # Convert rdd to format [user_id, [business_id_1, business_id_2...]]
    # Filter out users who have reviewed less than threshold business, since there will be no edges for these users
    user_business = review_idx.groupByKey().mapValues(list).filter(lambda x: len(x[1]) >= filter_threshold).collectAsMap()

    # Generate pairs

    # list_of_pairs = itertools.combinations(user_business.keys(), 2)
    list_of_pairs = sc.parallelize([i for i in user_business.keys()]).flatMap(lambda x: [(x, i) for i in user_business.keys() if i > x])
    list_of_pairs_filtered = list_of_pairs.\
        filter(lambda x: compute_similarity(user_business[x[0]], user_business[x[1]], filter_threshold))
    # Generate Nodes and Edges in RDD
    # nodes = list_of_pairs_filtered.flatMap(lambda x: x).distinct()
    edges = list_of_pairs_filtered.map(lambda x: [(x[0], x[1]), (x[1], x[0])]).flatMap(lambda x: x).distinct()

    # vertices_list = set() # avoid duplicates
    # edges_list = set() # need to add edges in both directions because the Edge dataframe in GraphFrames expects directed edges 
    # for pair in list_of_pairs_filtered:
    #     user_1 = pair[0]
    #     user_2 = pair[1]
    #     # Add vertices
    #     vertices_list.add((user_1,))
    #     vertices_list.add((user_2,))

    #     # Add edges
    #     edges_list.add((user_1, user_2)) # undirected graph
    #     edges_list.add((user_2, user_1))

    # # Convert the list of tuples to an RDD
    # vertices = sc.parallelize(vertices_list)
    # edges = sc.parallelize(edges_list)
    # nodes_neighbors = edges.groupByKey().mapValues(list).collectAsMap()
    nodes_neighbors = edges.groupByKey().mapValues(list)
    nodes_degree = nodes_neighbors.map(lambda x: (x[0], len(x[1]))).collectAsMap()
    nodes_neighbors = nodes_neighbors.collectAsMap()
    

    #############
    # Algorithm #
    #############
    edge_betweennes = girvan_newman_algorithm(nodes_neighbors)
    communities = find_communities(edge_betweennes, nodes_neighbors, nodes_degree)

    save_output_betweenness(edge_betweennes, betweenness_output_file_path, index_users_invert) # Task 2.1
    save_output_communities(communities, community_output_file_path, index_users_invert) # Tasl 2.2
    end_time = time.time()
    print('Duration: ', end_time - start_time)