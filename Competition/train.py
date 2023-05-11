from pyspark import SparkContext
import time
import json
import sys
import csv
import numpy as np
import xgboost as xgb
import re
import pandas as pd
import joblib
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold


# Function to remove special characters and convert to lowercase
def clean_text(text):
    if text:
        text = re.sub(r'[^\w\s]', ' ', text)
        text = text.lower()
        return text
    
# Function to check for the presence of category words
def check_category(text, category):
    for word in category:
        if text:
            if word in text:
                return word
    return 'other'

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
    folder_path = sys.argv[1]
    test_file_name = sys.argv[2]
    output_file_name = sys.argv[3]
    PATH_TO_MODELS_FOLDER = 'models2'
    N_SPLITS = 10 # number of stacked models

    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR')

    ################
    # Read Dataset #
    ################
    yelp_train = sc.textFile(f'{folder_path}/yelp_train.csv').zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(",")).map(lambda x: {'user_id': x[0], 'business_id': x[1], 'stars': x[2]})
    # yelp_val = sc.textFile(f'{test_file_name}').zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(",")).map(lambda x: {'user_id': x[0], 'business_id': x[1]})
    business = sc.textFile(f'{folder_path}/business.json')
    review_json = sc.textFile(f'{folder_path}/review_train.json')
    user = sc.textFile(f'{folder_path}/user.json')
    business = business.map(json.loads)
    review_json = review_json.map(json.loads)
    user = user.map(json.loads)


    ######################
    # Data Preprocessing #
    ######################

    # Business
    category = ['restaurants', 'shopping', 'food', 'beauty', 'health', 'home', 'nightlife', 'automotive', 'bars', 'local']
    business_metrics = business.map(lambda x:
                 {'business_id': x['business_id'], 'stars': x['stars'], 'latitude': x['latitude'],
                  'longitude': x['longitude'],
                  'review_count': x['review_count'],
                  'n_attributes': len(x['attributes']) if x['attributes'] else 0, 
                  'category': check_category(clean_text(x['categories']) if clean_text(x['categories']) else ' ', category)}) # get category of each business
    
    user_avg_per_category = review_json.\
    map(lambda x: (x['business_id'], x['stars'])).\
    leftOuterJoin(business_metrics.map(lambda x: (x['business_id'], x['category']))).map(lambda kv: (kv[1][1], kv[1][0]))
    user_avg_per_category = user_avg_per_category.aggregateByKey((0,0), lambda a,b: (a[0] + b, a[1] + 1),
                                            lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda x: x[0]/x[1]).map(lambda x: {'category': x[0], 'avg_star_category': x[1]})
    
    # User Metrics

    user_review_metrics = review_json.map(lambda x: (x['user_id'], x['business_id'])).\
        groupByKey().mapValues(lambda x: len(x)).\
                                    map(lambda x: {'user_id': x[0], 'review_count_user': x[1]})


    # Combining Features into X and y

    ## Merging Business_Metrics
    X_train = yelp_train.map(lambda x: (x['business_id'], (x['user_id'], float(x['stars'])))).leftOuterJoin(
        business_metrics.map(lambda x: (x['business_id'], (x['stars'], x['latitude'], x['longitude'], x['review_count'], x['n_attributes'], x['category'])))).\
        map(lambda x: {'business_id': x[0], 'user_id': x[1][0][0], 'stars': x[1][0][1],
                    'stars_business': x[1][1][0], 'latitude': x[1][1][1],
                    'longitude': x[1][1][2], 'review_count_business': x[1][1][3], 'n_attributes': x[1][1][4], 'category': x[1][1][5]})
    ## Merging User
    X_train_user = X_train.map(lambda x: (x['user_id'], (x['business_id'], x['stars'], x['stars_business'],
                                        x['latitude'], x['longitude'], x['review_count_business'],
                                        x['n_attributes'], x['category']))).\
                                            leftOuterJoin(
        user.map(lambda x: (x['user_id'], (
            x['review_count'], x['average_stars'], x['useful'], x['funny'], x['cool'], x['fans'], x['compliment_hot'],
            x['compliment_more'], x['compliment_profile'], x['compliment_cute'],
                                x['compliment_list'], x['compliment_note'], x['compliment_plain'],
                                x['compliment_cool'], x['compliment_funny'], x['compliment_writer'],
                                x['compliment_photos'],
                                int(x['yelping_since'][:4]))))
        )
    
    ## Merge Avg_star_category
    X_train_user = X_train_user.map(lambda x: (x[1][0][7], x[0:])).leftOuterJoin(
        user_avg_per_category.map(lambda x: (x['category'], x['avg_star_category']))
    )

    ## Rearranging columns
    X_train_user = X_train_user.flatMap(lambda x: [(x[1][0][0],) + x[1][0][1][0] + x[1][0][1][1] + (x[1][1], )])

    ## Merging User Metrics
    X_train_user = X_train_user.map(lambda x: (x[0], x[0:])).leftOuterJoin(
        user_review_metrics.map(lambda x: (x['user_id'], x['review_count_user']))
    ).flatMap(lambda x: [x[1][0] + (x[1][1],)])

    ## Naming dataset
    X_train_user_df = X_train_user.map(
        lambda x:
        {'user_id': x[0],'business_id': x[1], 
        'stars': x[2],
        'stars_business': x[3],
        'latitude': x[4], 'longitude': x[5],
        'review_count_business': x[6], 'n_attributes': x[7],
        'review_count': x[9],
        'average_stars_user': x[10],
        'useful': x[11],
        'funny': x[12],
        'cool': x[13],
        'fans': x[14], 'compliment_hot': x[15],
        'compliment_more': x[16], 'compliment_profile': x[17],
        'compliment_cute': x[18], 'compliment_list': x[19],
        'compliment_note': x[20], 'compliment_plain': x[21],
        'compliment_cool': x[22], 'compliment_funny': x[23],
        'compliment_writer': x[24], 'compliment_photos': x[25],
        'yelping_since_year': x[26], 'avg_star_category': x[27],
        'review_count_user': x[28]
        }
    )

    df_train = pd.DataFrame(X_train_user_df.collect())

    ###############
    # Fine-Tuning #
    ###############

    # Code used for finding the model parameters

    # params = {'max_depth': [7, 8, 9],
    #         'learning_rate': [0.01, 0.03 ,0.05, 0.07, 0.1],
    #         'n_estimators': [512],
    #             'colsample_bytree': np.arange(0.7, 1, 0.1),
    #             'colsample_bylevel': np.arange(0.7, 1.0, 0.1)}
    # clf = RandomizedSearchCV(estimator=model,
    #                      param_distributions=params,
    #                      scoring='neg_mean_squared_error',
    #                      n_iter=25,
    #                      random_state=42,
    #                      verbose=1)
    # clf.fit(X_train, y_train)
    # print("Best parameters:", clf.best_params_)
    # print("Lowest RMSE: ", (-clf.best_score_)**(1/2.0))

    ############
    # Training #
    ############

    model_features = ['stars_business', 'review_count_business', 'review_count','average_stars_user', 
                   'useful', 'funny', 'cool', 'fans', 'review_count_user','n_attributes',
                   'latitude', 'longitude', 'yelping_since_year', 
                   'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 
                   'compliment_list', 'compliment_note', 'compliment_plain', 
                   'compliment_cool', 'compliment_funny', 'compliment_writer','compliment_photos', 'avg_star_category']
    X_train = df_train[model_features]

    cv = KFold(n_splits=N_SPLITS, random_state=553, shuffle=True)
    X = df_train[model_features].copy()
    y = df_train['stars'].copy()

    for i, (train_index, test_index) in enumerate(cv.split(X)):
        print(f'--------Model {i}--------')
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]

        params = {'subsample': 0.7999999999999999,
        'n_estimators': 512,
        'max_depth': 7, 'learning_rate': 0.05,
        'colsample_bytree': 0.8999999999999999, 'colsample_bylevel': 0.7999999999999999}

        model = xgb.XGBRegressor(**params, verbosity=0, random=42)

        model.fit(X_train, y_train)
        y_pred = model.predict(X_train)
        mse = mean_squared_error(y_pred, y_train)
        print(f"Train RMSE: {(mse ** (1/2))}")

        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_pred, y_test)
        print(f"Eval RMSE: {(mse ** (1/2))}")

        joblib.dump(model, f"{PATH_TO_MODELS_FOLDER}/model_{i}_xgb.sav")
    
    end_time = time.time()
    print('Duration: ', end_time - start_time)

