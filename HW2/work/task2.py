from pyspark import SparkContext
import sys
from collections import defaultdict
import itertools
import time
import csv

def write_csv(rows: list, columns: list, output_path: str) -> None:
    '''Write rows to CSV

    Args:
        rows (list)
        columns (list)
        output_path (str)
    Returns:
        None
    '''
    with open(output_path, 'w') as csvfile:
        # creating a csv writer object 
        csvwriter = csv.writer(csvfile)

        # writing the fields
        csvwriter.writerow(columns)

        # writing the data rows 
        csvwriter.writerows(rows)


if __name__ == '__main__':
    # Read Arguments
    if len(sys.argv) != 5:
        print("Invalid Arguments")
        exit(1)

    filter_threshold = int(sys.argv[1])
    support_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]

    # Start SparkContext
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR') 
    # Read File, skipping header
    start_time = time.time()

    ######################
    # Data Preprocessing #
    ######################
    # Read File, skipping header
    ta_feng_data = sc.textFile(input_file_path).zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(","))

    # Select Columns: DATE (string), CUSTOMER_ID (int) and PRODUCT_ID (int)
    # [1:-1] -> Remove "" 
    ta_feng_data = ta_feng_data.map(lambda x: (x[0][1:-1], int(x[1][1:-1]), int(x[5][1:-1])))
    ta_feng_data_preprocessed = ta_feng_data.map(lambda x: (f"{x[0]}-{x[1]}", x[2]))

    # Write to CSV
    rows = ta_feng_data_preprocessed.collect()
    file_path_output = 'customer_product.csv'
    fields = ["DATE-CUSTOMER_ID", "PRODUCT_ID"]
    write_csv(rows, fields, file_path_output)