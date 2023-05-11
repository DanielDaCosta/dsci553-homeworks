######################
# Method Description #
######################

# Model:
# In this project, I have implemented a Model-Based approach to predict the user's rating.
# The model I have used is an XGBoost, which has been fine-tuned using a RandomizedSearchCV with a set of carefully chosen parameters.
# The hyperparameters that have been tuned were 
# {'max_depth': [7, 8, 9], 'learning_rate': [0.01, 0.03 ,0.05, 0.07, 0.1], 'n_estimators': [512],
# 'colsample_bytree': np.arange(0.7, 1, 0.1), 'colsample_bylevel': np.arange(0.7, 1.0, 0.1).
# I have used a Model Stacking technique with Cross Validation to train the model, which has produced 10 different models.
# I have then averaged each of their predictions to obtain the final rating prediction.
# To ensure consistency and reproducibility, I have saved each of the 10 models in the model/ folder and read them using the joblib library to make the prediction.
# I have also experimented with combining a user-based, item-based with the model-based prediction using both switching and weighting techniques.
# However, I have found that the model-based prediction outperformed the other two approaches.

# Featue Engineering:
# In this project, I have invested a lot of effort into feature engineering to find the best features that would lead to the lowest RMSE.
# I have used both raw features from the User and Business dataset as well as new created features. The features I have created include:
# - n_attributes: number of attributes of each business_id
# - average_stars_user: average star rating of each user 
# - avg_star_category: average star rating of each user for each category of business.
# Each business was categorized in one of the following categories: 
# category = ['restaurants', 'shopping', 'food', 'beauty', 'health', 'home', 'nightlife', 'automotive', 'bars', 'local']
# - yelping_since_year:  year of each review
# - review_count_business: average number of reviews per business


###########
# Metrics #
###########

# Metrics on Validation Set

########
# RMSE #
########

# RMSE:
# 0.9772904711772428


######################
# Error Distribution #
######################

# Error Distribution:
# >=0 and <1: 102388
# >=1 and <2: 32730
# >=2 and <3: 6097
# >=3 and <4: 828
# >=4: 0

##################
# Execution Time #
##################

# Execution Time:
# 91.40778517723083s


from pyspark import SparkContext
import time
import json
import sys
import csv
import numpy as np
# import xgboost as xgb
import re
import pandas as pd
import joblib


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
    PATH_TO_MODELS_FOLDER = 'models_072'
    N_SPLITS = 10 # number of stacked models

    start_time = time.time()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('ERROR')

    ################
    # Read Dataset #
    ################
    # yelp_train = sc.textFile(f'{folder_path}/yelp_train.csv').zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(","))
    yelp_val = sc.textFile(f'{test_file_name}').zipWithIndex().filter(lambda x: x[1] > 0).map(lambda line: line[0].split(",")).map(lambda x: {'user_id': x[0], 'business_id': x[1]})
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
    


    #############################################################
    # Filter Users_id or Business_id not found in training data #
    #############################################################
    # Filter users missing business
    users_with_no_business = yelp_val.map(lambda x: (x['business_id'], x['user_id'])).subtractByKey(business.map(lambda x: (x['business_id'], None)))
    users_with_no_user_id = yelp_val.map(lambda x: (x['user_id'], x['business_id'])).subtractByKey(user.map(lambda x: (x['user_id'], None))) # (user_id, business_id)

    users_with_no_business_and_no_user_id = sc.parallelize([])
    if not users_with_no_business.isEmpty():
        users_with_no_business = users_with_no_business.map(lambda x: (x[1], x[0])) # (user_id, business_id)
        users_with_no_business_and_no_user_id = users_with_no_business.map(lambda x: (x[0], x[1])).subtractByKey(user.map(lambda x: (x['user_id'], None))) # (user_id, business_id)

    users_with_no_user_id = users_with_no_user_id.subtract(users_with_no_business_and_no_user_id)
    users_with_no_business = users_with_no_business.subtract(users_with_no_business_and_no_user_id)

    # Remove user that don't have user_id and business_id on training data
    yelp_val_filtered = yelp_val.map(lambda x: (x['user_id'], x['business_id'])).subtract(users_with_no_business_and_no_user_id).\
    subtract(users_with_no_user_id).subtract(users_with_no_business).map(lambda x: {'user_id': x[0], 'business_id': x[1]})
    
    # Combining Features into X and y

    ## Merging Business_Metrics
    X_val = yelp_val_filtered.map(lambda x: (x['business_id'], x['user_id'])).leftOuterJoin(
        business_metrics.map(lambda x: (x['business_id'], (x['stars'], x['latitude'], x['longitude'], x['review_count'], x['n_attributes'], x['category'])))).\
        map(lambda x: {'business_id': x[0], 'user_id': x[1][0],
                    'stars_business': x[1][1][0], 'latitude': x[1][1][1],
                    'longitude': x[1][1][2], 'review_count_business': x[1][1][3], 'n_attributes': x[1][1][4], 'category': x[1][1][5]})
    
    ## Merging User
    X_val_user = X_val.map(lambda x: (x['user_id'], (x['business_id'], x['stars_business'],
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
    X_val_user = X_val_user.map(lambda x: (x[1][0][6], x[0:])).leftOuterJoin(
        user_avg_per_category.map(lambda x: (x['category'], x['avg_star_category']))
    )

    ## Rearranging columns
    X_val_user = X_val_user.flatMap(lambda x: [(x[1][0][0],) + x[1][0][1][0] + x[1][0][1][1] + (x[1][1], )])

    ## Merging User Metrics
    X_val_user = X_val_user.map(lambda x: (x[0], x[0:])).leftOuterJoin(
        user_review_metrics.map(lambda x: (x['user_id'], x['review_count_user']))
    ).flatMap(lambda x: [x[1][0] + (x[1][1],)])

    ## Naming dataset
    X_val_user_df = X_val_user.map(
        lambda x:
        {'user_id': x[0],'business_id': x[1], 
        'stars_business': x[2],
        'latitude': x[3], 'longitude': x[4],
        'review_count_business': x[5], 'n_attributes': x[6],
        'review_count': x[8],
        'average_stars_user': x[9],
        'useful': x[10],
        'funny': x[11],
        'cool': x[12],
        'fans': x[13], 'compliment_hot': x[14],
        'compliment_more': x[15], 'compliment_profile': x[16],
        'compliment_cute': x[17], 'compliment_list': x[18],
        'compliment_note': x[19], 'compliment_plain': x[20],
        'compliment_cool': x[21], 'compliment_funny': x[22],
        'compliment_writer': x[23], 'compliment_photos': x[24],
        'yelping_since_year': x[25], 'avg_star_category': x[26],
        'review_count_user': x[27]
        }
    )



    # Add for users with No Business
    X_users_with_no_user_id = sc.parallelize([])
    X_users_with_no_user_df = sc.parallelize([])
    if not users_with_no_user_id.isEmpty():
        ## Merging Business_Metrics
        X_users_with_no_user_id = users_with_no_user_id.map(lambda x: (x[1], x[0])).leftOuterJoin(
            business_metrics.map(lambda x: (x['business_id'], (x['stars'], x['latitude'], x['longitude'], x['review_count'], x['n_attributes'], x['category'])))).\
            map(lambda x: {'business_id': x[0], 'user_id': x[1][0],
                        'stars_business': x[1][1][0], 'latitude': x[1][1][1],
                    'longitude': x[1][1][2], 'review_count_business': x[1][1][3], 'n_attributes': x[1][1][4], 'category': x[1][1][5]})
        X_users_with_no_user_id = X_users_with_no_user_id.map(lambda x: (x['category'],
            (x['business_id'], x['user_id'], x['stars_business'], x['latitude'], x['longitude'],
            x['review_count_business'], x['n_attributes'], x['category']))).leftOuterJoin(
            user_avg_per_category.map(lambda x: (x['category'], x['avg_star_category']))
        )

    ## Rearranging columns
    # X_users_with_no_user_id = X_users_with_no_user_id.flatMap(lambda x: [(x[1][0][0],) + x[1][0][1][0] + x[1][0][1][1] + (x[1][1], )])
    X_users_with_no_user_id = X_users_with_no_user_id.flatMap(lambda x: [x[1][0][0:] + (x[1][1],)])

    X_users_with_no_user_df = X_users_with_no_user_id.map(
        lambda x:
        {'user_id': x[1],'business_id': x[0], 
        'stars_business': x[2],
        'latitude': x[3], 'longitude': x[4],
        'review_count_business': x[5], 'n_attributes': x[6],
        'review_count': np.nan,
        'average_stars_user': np.nan,
        'useful': np.nan,
        'funny': np.nan,
        'cool': np.nan,
        'fans': np.nan, 'compliment_hot': np.nan,
        'compliment_more': np.nan, 'compliment_profile': np.nan,
        'compliment_cute': np.nan, 'compliment_list': np.nan,
        'compliment_note': np.nan, 'compliment_plain': np.nan,
        'compliment_cool': np.nan, 'compliment_funny': np.nan,
        'compliment_writer': np.nan, 'compliment_photos': np.nan,
        'yelping_since_year': np.nan, 'avg_star_category': x[8],
        'review_count_user': np.nan
        }
    )

    # Features for users with USER_ID or BUSINESS_ID not in training data
    X_users_with_no_business = sc.parallelize([])
    X_users_with_no_business_df = sc.parallelize([])
    if not users_with_no_business.isEmpty():
        ## Merging User
        X_users_with_no_business = users_with_no_business.\
                                                leftOuterJoin(
            user.map(lambda x: (x['user_id'], (
                x['review_count'], x['average_stars'], x['useful'], x['funny'], x['cool'], x['fans'], x['compliment_hot'],
                x['compliment_more'], x['compliment_profile'], x['compliment_cute'],
                                    x['compliment_list'], x['compliment_note'], x['compliment_plain'],
                                    x['compliment_cool'], x['compliment_funny'], x['compliment_writer'],
                                    x['compliment_photos'],
                                    int(x['yelping_since'][:4]))))
            )
        X_users_with_no_business = X_users_with_no_business.flatMap(lambda x: [(x[0],) + (x[1][0],)  + x[1][1]])
        # Merging User Metrics
        X_users_with_no_business = X_users_with_no_business.map(lambda x: (x[0], x[0:])).leftOuterJoin(
            user_review_metrics.map(lambda x: (x['user_id'], x['review_count_user']))
        ).flatMap(lambda x: [x[1][0] + (x[1][1],)])

        X_users_with_no_business_df = X_users_with_no_business.map(
            lambda x:
            {'user_id': x[0],'business_id': x[1], 
            'stars_business': np.nan,
            'latitude': np.nan, 'longitude': np.nan,
            'review_count_business': np.nan, 'n_attributes': np.nan,
            'review_count': x[2],
            'average_stars_user': x[3],
            'useful': x[4],
            'funny': x[5],
            'cool': x[6],
            'fans': x[7], 'compliment_hot': x[8],
            'compliment_more': x[9], 'compliment_profile': x[10],
            'compliment_cute': x[11], 'compliment_list': x[12],
            'compliment_note': x[13], 'compliment_plain': x[14],
            'compliment_cool': x[15], 'compliment_funny': x[16],
            'compliment_writer': x[17], 'compliment_photos': x[18],
            'yelping_since_year': x[19], 'avg_star_category': np.nan,
            'review_count_user': x[20]
            }
        )

    X_users_with_no_user_and_no_business = sc.parallelize([])
    X_users_with_no_user_and_no_business_df = sc.parallelize([])
    if not users_with_no_business_and_no_user_id.isEmpty():
        X_users_with_no_user_and_no_business_df = users_with_no_business_and_no_user_id.map(
            lambda x:
            {'user_id': x[0],'business_id': x[1], 
            'stars_business': np.nan,
            'latitude': np.nan, 'longitude': np.nan,
            'review_count_business': np.nan, 'n_attributes': np.nan,
            'review_count': np.nan,
            'average_stars_user': np.nan,
            'useful': np.nan,
            'funny': np.nan,
            'cool': np.nan,
            'fans': np.nan, 'compliment_hot': np.nan,
            'compliment_more': np.nan, 'compliment_profile': np.nan,
            'compliment_cute': np.nan, 'compliment_list': np.nan,
            'compliment_note': np.nan, 'compliment_plain': np.nan,
            'compliment_cool': np.nan, 'compliment_funny': np.nan,
            'compliment_writer': np.nan, 'compliment_photos': np.nan,
            'yelping_since_year': np.nan, 'avg_star_category': np.nan,
            'review_count_user': np.nan
            }
        )
    df_val = pd.DataFrame(X_val_user_df.collect())

    X_users_with_no_user_df = X_users_with_no_user_df.collect()
    if len(X_users_with_no_user_df):
        df_val_no_user = pd.DataFrame(X_users_with_no_user_df)
        df_val = pd.concat([df_val, df_val_no_user])
    X_users_with_no_business_df = X_users_with_no_business_df.collect()
    if len(X_users_with_no_business_df):
        df_val_no_business = pd.DataFrame(X_users_with_no_business_df)
        df_val = pd.concat([df_val, df_val_no_business])

    X_users_with_no_user_and_no_business_df = X_users_with_no_user_and_no_business_df.collect()
    if len(X_users_with_no_user_and_no_business_df):
        df_val_no_user_no_business = pd.DataFrame(X_users_with_no_user_and_no_business_df)
        df_val = pd.concat([df_val, df_val_no_user_no_business])

    ##############
    # Prediction #
    ##############

    model_features = ['stars_business', 'review_count_business', 'review_count','average_stars_user', 
                   'useful', 'funny', 'cool', 'fans', 'review_count_user','n_attributes',
                   'latitude', 'longitude', 'yelping_since_year', 
                   'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 
                   'compliment_list', 'compliment_note', 'compliment_plain', 
                   'compliment_cool', 'compliment_funny', 'compliment_writer','compliment_photos', 'avg_star_category']
    X_val = df_val[model_features]
    result = []
    for model_i in range(N_SPLITS):
        model = joblib.load(f"{PATH_TO_MODELS_FOLDER}/model_{model_i}_xgb.sav")
        y_pred = model.predict(X_val)
        result.append(y_pred)
    stacked_arr = np.vstack(result)
    y_pred = np.mean(stacked_arr, axis=0)
    df_val['prediction'] = y_pred

    save_csv(df_val[['user_id', 'business_id', 'prediction']].values, output_file_name)
    end_time = time.time()
    print('Duration: ', end_time - start_time)

