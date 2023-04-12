import random
import csv
import sys
from blackbox import BlackBox

SIZE_MEMORY = 100
SEQUENCE_N_USER = 0
RESEVOIR = []

def fixed_size_sampling(stream_users: list) -> None:
    '''Reservoir Sampling Algorithm

    Args:
        stream_users (list): list of users
        reservoir (list): Modify this list
    Returns:
        None
    '''
    global SEQUENCE_N_USER
    global RESEVOIR
    global SIZE_MEMORY
    for user in stream_users:
        SEQUENCE_N_USER += 1 #n_th user

        if len(RESEVOIR) <  SIZE_MEMORY:
            RESEVOIR.append(user)
        else:
            # accept the sample
            if random.random() < SIZE_MEMORY/SEQUENCE_N_USER:
                find_index_to_replace = random.randint(0, SIZE_MEMORY-1)
                # print(find_index_to_replace)
                # print(user)
                # print(RESEVOIR[find_index_to_replace])
                RESEVOIR[find_index_to_replace] = user


def save_csv(row: list, output_file_name: str, batch: int, header=['seqnum', '0_id', '20_id', '40_id', '60_id', '80_id']):
    if batch == 0: # create file with header
        with open(output_file_name, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows([header])
            # Write the data rows
            # for key, value in rows.items():
            writer.writerow(row)
    else: # append row
        with open(output_file_name, "a", newline="") as f:
            writer = csv.writer(f)
            # Write the data rows
            # for key, value in rows.items():
            writer.writerow(row)

if __name__ == '__main__':

    # Read Arguments
    if len(sys.argv) != 5:
        print("Invalid Arguments")
        exit(1)
    input_file_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_ask = int(sys.argv[3])
    output_file_path = sys.argv[4]

    random.seed(553) 
    # seed provided in the question

    bx = BlackBox()
    for batch in range(num_of_ask):
        # print(_)
        stream_users = bx.ask(input_file_path, stream_size)
        fixed_size_sampling(stream_users)
        # estimate = flajolet_martin_algorithm(stream_users, N_PARTITIONS)
        # myhashs()
        # users.extend(stream_users)
        # fpr = bloom_filter(stream_users)
        # unique_users = len(set(stream_users))
        # dict_results[batch] = [unique_users, estimate]
        save_items = [(batch+1)*stream_size, RESEVOIR[0], RESEVOIR[20], RESEVOIR[40], RESEVOIR[60], RESEVOIR[80]]
        save_csv(save_items, output_file_path, batch)