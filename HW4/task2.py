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
                    if parent == 'A':
                        print(bfs_target_node[parent])
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

        f.write('\n'.join(f"('{key_tuple[0]}', '{key_tuple[1]}'),{value}" for key_tuple, value in sorted(rows.items(), key= lambda x: (-x[1], x[0]))))
                                                                                                                                                                           


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

    list_of_pairs = itertools.combinations(user_business.keys(), 2)

    vertices_list = set() # avoid duplicates
    edges_list = set() # need to add edges in both directions because the Edge dataframe in GraphFrames expects directed edges 
    for pair in list_of_pairs:
        user_1 = pair[0]
        user_2 = pair[1]
        if compute_similarity(user_business[user_1], user_business[user_2], filter_threshold):
            # Add vertices
            vertices_list.add(user_1)
            vertices_list.add(user_2)

            # Add edges
            edges_list.add((user_1, user_2)) # undirected graph
            edges_list.add((user_2, user_1))

    # Convert the list of tuples to an RDD
    vertices = sc.parallelize(vertices_list)
    edges = sc.parallelize(edges_list)
    nodes_neighbors = edges.groupByKey().mapValues(list).collectAsMap()

    #############
    # Algorithm #
    #############
    edge_betweennes = girvan_newman_algorithm(nodes_neighbors)


    save_output_betweenness(edge_betweennes, betweenness_output_file_path, index_users_invert)
    end_time = time.time()
    print('Duration: ', end_time - start_time)