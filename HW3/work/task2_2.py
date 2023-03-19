from pyspark import SparkContext
import time
import sys
import numpy as np
import xgboost as xgb
import json
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
    if len(sys.argv) != 4:
        print("Invalid Arguments")
        exit(1)
    folder_path = sys.argv[1]
    test_file_name = sys.argv[2]
    output_file_name = sys.argv[3]

    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN') 


    ################
    # Read Dataset #
    ################
    yelp_train = sc.textFile(f'{folder_path}/yelp_train.csv').zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(","))
    business = sc.textFile(f'{folder_path}/business.json')
    business = business.map(json.loads)
    review_json = sc.textFile(f'{folder_path}/review_train.json')
    review_json = review_json.map(json.loads)

    ######################
    # Data Preprocessing #
    ######################
    # Compute Avg_Star and Review Count per User_Id
    review_json_avg_star_review_count = review_json.map(
        lambda x: (x['user_id'], x['stars'])).aggregateByKey((0,0), lambda a,b: (a[0] + b, a[1] + 1),
                                        lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda x: (x[0]/x[1], x[1]))

    # Compute Number of like, funny and cool reviews per user
    review_json_sum_opinions = review_json.map(
        lambda x: (x['user_id'], (x['useful'], x['funny'], x['cool']))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
    
    # Add Business Stars and review_count
    # (business_id, user_id, avg_stars, review_count)
    train_rdd = yelp_train.map(lambda x: (x[1], (x[0], float(x[2])))).join(
        business.map(lambda x: (x['business_id'], (x['stars'], x['review_count']))
    )).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))

    train_rdd = train_rdd.map(lambda x: (x[1], (x[0], x[2], x[3], x[4]))).join(
    review_json_avg_star_review_count).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][1][0], x[1][1][1]))

    train_rdd = train_rdd.map(lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5], x[6]))).\
    join(review_json_sum_opinions).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2],x[1][0][3], x[1][0][4], x[1][0][4], x[1][1][0], x[1][1][1], x[1][1][2]))

    # Shuffling
    train_rdd = train_rdd.map(lambda x: (hash(x), x)).sortByKey().map(lambda x: x[1])


    ############
    # Training #
    ############
    X = np.array(train_rdd.map(lambda x: [x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9]]).collect())
    # X_train = np.array(train_rdd.map(lambda x: [x[3], x[4], x[5], x[6], x[7], x[8], x[9]]).collect())
    X_train = X[:, 1:]
    y_train = X[:, 0]
    xgbr = xgb.XGBRegressor(verbosity=0, eta=0.15, max_depth=6, seed=42, reg_lambda=3, eval_metric='rmse')
    xgbr.fit(X_train, y_train)


    ##############
    # Prediction #
    ##############
    yelp_val = sc.textFile(f'{test_file_name}').zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(","))
    # Add Business Stars and review_count
    # (business_id, user_id, avg_stars, review_count)
    test_rdd = yelp_val.map(lambda x: (x[1], x[0])).join(
        business.map(lambda x: (x['business_id'], (x['stars'], x['review_count']))
    )).map(lambda x: (x[0], x[1][0], x[1][1][0], x[1][1][1]))
    test_rdd = test_rdd.map(lambda x: (x[1], (x[0], x[2], x[3]))).join(
    review_json_avg_star_review_count).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][0], x[1][1][1]))
    test_rdd = test_rdd.map(lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5]))).\
    join(review_json_sum_opinions).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2],x[1][0][3], x[1][0][4], x[1][1][0], x[1][1][1], x[1][1][2]))


    X_test = np.array(test_rdd.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8]]).collect())
    y_pred = xgbr.predict(X_test[:, 2:].astype(float))
    X_prediction = sc.parallelize(np.concatenate((X_test[:, :2], y_pred.reshape(X_test.shape[0], 1)), axis=1))

    rows = X_prediction.collect()
    # file_path_output = 'task2_2_2.csv'
    fields = ["user_id", "business_id", "prediction"]
    write_csv(rows, fields, output_file_name)


    end_time = time.time()
    print('Duration: ', end_time - start_time)
