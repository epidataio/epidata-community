#
# Copyright (c) 2015-2017 EpiData, Inc.
#

# from data_frame import DataFrame
from cmath import isnan
import pandas as pd
import math
import numbers
import copy
import numpy as np
import json

from datetime import datetime
import pandas as pd

import argparse
import base64
from datetime import datetime, timedelta
# import httplib
import json
import numpy as np
import random
from decimal import Decimal
import struct
import time
from time import sleep
# ! pip install holidays
import holidays

# !pip install lightgbm
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()

from sklearn.model_selection import KFold, StratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

# from sklearn.keras.utils import np_utils
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import make_column_transformer
import tensorflow as tf
from tensorflow.keras.layers import Dropout
import sklearn.metrics as metrics
import optuna
from optuna.integration import LightGBMPruningCallback
from optuna import Trial

def resample(df : pd.core.frame.DataFrame, fields : list, time_interval: int, time_unit: str):    
    
    """
    df : DataFrame
    fields : list of meas_names
    time_interval: integer: 1, 60, 120...
    time_unit: string 
    
    eg: 'H' -> hourly frequency
        'T', 'min' -> minutely frequency
        'S' -> secondly frequency
        more frequency strings through this link: 
        https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases   
    """
    df['meas_value'] = df['meas_value'].apply(lambda x : float(x))
    
    # Transform ['ts'] into datetime
    df['ts'] = df['ts'].apply(lambda x : datetime.fromtimestamp(int(x)/1e3))
    
    # Make dataframe with and without the elements in list(fields)
    filtered = df[df['meas_name'].isin(fields)]
    not_filtered = df[~df['meas_name'].isin(fields)]
    
    # Create times series in the frequency of input
    temp_series = pd.DataFrame()
    freq = str(time_interval) + str(time_unit)
    temp_series['ts'] = pd.date_range(df['ts'].min(), df['ts'].max(), freq=freq)
    
    # Filtered dataframe will be updated through merge series in wanted frequency
    filtered = filtered.merge(temp_series, on = 'ts' , how = 'left')
    
    # Concat both filtered fields and unfiltered fileds.
    df = pd.concat([filtered, not_filtered]).reset_index()

    df['ts'] = df['ts'].apply(lambda x : int(time.mktime(x.timetuple()) * 1000+ x.microsecond/1000))
    # df['ts'] = [x.timestamp() for x in df['ts']]

    return df
    

    
def outliers(df, meas_names: list, percentage: float, method: str):
    """
    meas_names : string of meas_names, eg: 'Temperature'
    percentage: float -> the upper and lower quantile that want to be considered as outliers, usually use 0.05 or 0.1
    method: string -> could be 'average', 'detete'
            'average' means subsitute with average value of selected field
            'delete' means delete outliers
    """
    
    for field in meas_names:
        
        q_low = df[df['meas_name'] == field]['meas_value'].quantile(percentage)
        q_high  = df[df['meas_name'] == field]['meas_value'].quantile(1-percentage)

        indices = df.loc[df["meas_name"] == field].index[df.loc[df["meas_name"] == field]["meas_value"].apply(lambda x: x < q_low or x > q_high)]


        if method == 'delete':
                                                         
            df = df.drop(indices)
                                                         
        elif method == 'average':
                                                         
            df.loc[indices,['meas_value']] = df[df['meas_name'] == field]['meas_value'].mean()[0]

    return df




def missing_substitute(df, meas_names, method="rolling", size=3):
    """
    Substitute missing measurement values within a data frame, using the specified method.

    Parameters
    ----------
    df : pandas DataFrame
        A DataFrame in which to detect missing values.
    meas_name : list of strings
        The names of the measurements within df in which to detect missing values and perform substitution.
        The columns must contain numeric values.
    method : string
        The name of the substitution method. Available methods include:
        'rolling'.
    size: int
        The window size for calculating moving average.

    Returns
    -------
    result : pandas DataFrame
        A DataFrame containing full copies of the substituted and non-substituted rows within df. Some substitution methods may add a new column to the result to indicate that substitution was perfomed.
    """
    df["meas_value"] = df["meas_value"].apply(
        lambda x: x if (
            not isinstance(
                x,
                str) and not (
                x is None or np.isnan(x))) or (
                    isinstance(
                        x,
                        str) and x != "") else np.nan)

    drop = []
     # If too much missing in columns, just delete them
    # data_ratios = df.count()/len(df)
    # for i in range(0,len(data_ratios)):
    #     if data_ratios[i] < 0.2:      
    #         drop.append(data_ratios.index[i])  
            
    df = df.drop(drop, axis = 1)
    for meas_name in meas_names:

        if (method == "rolling"):
            if ((size % 2 == 0) and (size != 0)):
                size += 1
            if df.loc[df["meas_name"] == meas_name].size > 0:
                indices = df.loc[df["meas_name"] == meas_name].index[df.loc[df["meas_name"] == meas_name]["meas_value"].apply(
                    lambda x: not isinstance(x, str) and (x is None or np.isnan(x)))]
                substitutes = df.loc[df["meas_name"] == meas_name]["meas_value"].rolling(
                    window=size, min_periods=1, center=True).mean()

                df["meas_value"].fillna(substitutes, inplace=True)
                df.loc[indices, "meas_flag"] = "substituted"
                df.loc[indices, "meas_method"] = "rolling average"
        else:
            raise ValueError("Unsupported substitution method: ", repr(method))

    return df

def creat_template(df, meas_names = []):
    """"
    creat a new df which contains the structure of origainl df
    keeps meas_name and corresponding sensor and meas_unit
    this step is in order to merge into the prediction dataframe and helps to store in the db
    """
    temp = df.copy()
    temp['ts'] = np.nan
    temp['event'] = np.nan
    temp['meas_value'] = np.nan
    temp['meas_datatype'] = np.nan
    temp['meas_status'] = np.nan
    temp['meas_lower_limit'] = np.nan
    temp['meas_upper_limit'] = np.nan
    temp['meas_description'] = np.nan
    
    temp = temp[temp.columns.tolist()].drop_duplicates(subset = temp.columns.tolist()).reset_index(drop = True)
    return temp 

def transpose(df : pd.core.frame.DataFrame , meas_drop_columns : list):
    """
    df : DataFrame
    Note:
    If df is for training, df should be called by resample, outliers, missing_subsitute.
    If df is for prediction, df should just be called by current function
    
    meas_drop_columns : list of meas_names, if no leave it empty -> []
    """
    
    # Extract keys dataframe without duplicate
    only_keys = df[['company', 'site', 'station','ts']].drop_duplicates(subset = ['company', 'site', 'station','ts']).reset_index(drop = True)
    
    
    new_df = pd.DataFrame()
    
    for i in range(only_keys.shape[0]):
        
        # Merge each row of only_keys dataframe with the input dataframe 
        # In each row, do the transpose for all the meas_name and meas_value 
        temp = only_keys[i : i + 1].merge(df, how = 'left')[['meas_name', 'meas_value']]\
                               .transpose().reset_index().drop(columns = 'index')
        
        # Create the list of column names in order to reset the col name of dataframe
        temp.columns = temp[ : ].transpose()[0].tolist()
        
        # Concact new rows together
        new_df = pd.concat([ pd.concat([only_keys[i : i+1].reset_index(drop = True)\
                , temp.drop([0]).reset_index(drop = True)], axis = 1), new_df ]).reset_index(drop = True)

        
    if len(meas_drop_columns) != 0:
        new_df = new_df.drop(columns = meas_drop_columns)
    
    return new_df

def feature_engineering(df, meas_names = [], add = False, multiply = False):
    
    """
  
    Add some feature such as time features: hour, day, weekday..and featrues like if itis holiday.
    More features can be added under different situations like if there is any event etc.
    """
    
    df['hour'] = df['ts'].apply(lambda x : x.hour)
    df['day'] =df['ts'].apply(lambda x : x.day)
    df['weekday'] =df['ts'].apply(lambda x : x.weekday())
    df['month'] =df['ts'].apply(lambda x : x.month)
    df['year'] =df['ts'].apply(lambda x : x.year)
    # df['microsecond'] = df['ts'].apply(lambda x : x.microseconds)
    
    def is_holiday(x):
        if x in holidays.US():
            return 1
        else:
            return 0
        
    df['Holiday'] = df['ts'].apply(is_holiday)
    
    if add != False:
        df['new_1'] = 0
        for i in range (0, len(add)):
            df['new_1'] = df['new_1'] + df[str(add[i])]
    
    if multiply != False:
        df['new_2'] = 1
        for i in range (0, len(multiply)):
            df['new_2'] = df['new_2'] * df[str(multiply[i])]

    
    
    
    return df



def prediction(train_df, predict_df, categorical_features:list, target: str, fold: int, trails: int):
    
    """
    train_df: The dataframe after calling feature_engineering, including variables and target value
    predict_df: The dataframe after calling feature_engineering, with variables but without target value
    categorical_features:list
    target: str
    fold: int
    trails: int
    """
    
    dropped = ['company','site','station','ts']
    train_df = train_df.drop(columns=dropped, axis = 1)
    train = train_df.astype(float)
    categorical_features = categorical_features
    for i in categorical_features:
        train[i] = le.fit_transform(train[i])
#     print(train.info)

    predict = predict_df.drop(columns=dropped, axis = 1)
    predict = predict.astype(float)
    categorical_features = categorical_features
    for i in categorical_features:
        predict[i] = le.fit_transform(predict[i])
#     print(predict.info)
    
    
#     print(train)
    target  = target
    fold = fold
    trails = trails
    X = train.drop(columns=[target], axis = 1)
    y = train[target].values

    
    def fit_lgbm(trial, train, val):
    
        X_train, y_train = train
        X_valid, y_valid = val

        #Auto find hyperparameters
        params = {
            "boosting": "gbdt",
            "objective": "regression",
            "num_leaves": trial.suggest_int("num_leaves", 20, 256),
            "max_depth": trial.suggest_int("max_depth", 3, 12),
            'lambda_l2': trial.suggest_loguniform('lambda_l2', 1e-8, 10.0),
            "feature_fraction": 0.85,
            "learning_rate": 0.05,
            "metric": "rmse",
        }

        d_training = lgb.Dataset(X_train, label=y_train,categorical_feature=categorical_features, free_raw_data=False)
        d_test = lgb.Dataset(X_valid, label=y_valid,categorical_feature=categorical_features, free_raw_data=False)


        pruning_callback = optuna.integration.LightGBMPruningCallback(trial, 'rmse', valid_name='valid_1')    
        model = lgb.train(params, train_set=d_training, num_boost_round=1000, valid_sets=[d_training,d_test], verbose_eval=25, early_stopping_rounds=20)

        y_pred_valid = model.predict(X_valid, num_iteration=model.best_iteration)

        best_val_score = model.best_score['valid_1']['rmse']
        return model, y_pred_valid, best_val_score
    
    
    def objective(trial : Trial, return_info = False):
        folds = fold
        kf = KFold(n_splits=folds, shuffle=False, random_state=None)
        y_valid_pred_total = np.zeros(X.shape[0])

        models = []
        valid_score = 0

        for train_idx, valid_idx in kf.split(X, y):
            train_data = X.iloc[train_idx,:], y[train_idx]
            valid_data = X.iloc[valid_idx,:], y[valid_idx]

            model, y_pred_valid, score = fit_lgbm(trial, train_data, valid_data)
            y_valid_pred_total[valid_idx] = y_pred_valid
            models.append(model)
            valid_score += score
        valid_score /= len(models)

        if return_info:
            return valid_score, models, y_pred_valid, y
        else:
            return valid_score
        
    study = optuna.create_study(pruner=optuna.pruners.SuccessiveHalvingPruner(min_resource=2, reduction_factor=4, min_early_stopping_rate=1))
    study.optimize(objective, n_trials=trails)
    
    best = study.best_params
    valid_score, models, y_pred_valid, y_train = objective(optuna.trial.FixedTrial(best), return_info = True)
    results = []
    
    for model in models:
        if  results == []:
            results = model.predict(predict, num_iteration=model.best_iteration, predict_disable_shape_check=True)/ len(models)
        else:
            results += model.predict(predict, num_iteration=model.best_iteration, predict_disable_shape_check=True) / len(models)
            
            
            
    predict_df[target] = results
            
            
    return predict_df

def inverse_transpose(result_df,original_df, meas_col:list):
    """
    result_df: the dataframe after calling prediction fuction 
    original_df : has the full columns as the original input
    meas_col:including all the measments target and varibales
        
    """
    print("RESULT DF: ", result_df)
    print("ORIGINAL DF: ", original_df)
    temp1 = pd.DataFrame({'meas_name' :meas_col})
    temp1['value'] = 1
    result_df['value'] =1
    result_df = result_df.merge(temp1, on = 'value', how = 'left').drop(columns = 'value')
    result_df['meas_value'] = result_df.apply(lambda x : x[x['meas_name']], axis=1)
    result_df = result_df[['company','site','station','ts', 'meas_name', 'meas_value']]
    # result_df['ts'] = result_df['ts'].apply(lambda x : int(x))
    result_df = original_df.merge(result_df, how = 'right', on = ['company','site','station','ts', 'meas_name'])

    return result_df



# def IMR(measurements, meas_names=None):
#     """
#     Perform IMR analysis on a DataFrame of measurements. The measurements are
#     grouped by the 'meas_name' field and IMR is performed on each group.

#     Parameters
#     ----------
#     measurements : epidata DataFrame
#         A DataFrame containing measurements, as returned by EpidataContext.query.
#     meas_names : list of strings, or string or None, default None
#         A list of measurement names on which to perform IMR, or a single
#         measurement name on which to perform IMR. If None, all measurements will
#         be analyzed.

#     Returns
#     -------
#     result : epidata DataFrame
#         A copy of the measurements DataFrame, with the IMR results appended as
#         additional columns.
#     """

#     from context import ec

#     local = not isinstance(measurements, DataFrame)
#     if local:
#         raise ValueError('Unsupported local measurements argument to IMR.')

#     if isinstance(meas_names, basestring):
#         # Filter a single string measurement name.
#         measurements = measurements.filter(
#             measurements.meas_name == meas_names)
#     elif meas_names:
#         # Build a composite filter for a list of measurement names.
#         condition = (measurements.meas_name == meas_names[0])
#         for name in meas_names[1:]:
#             condition = (condition | (measurements.meas_name == name))
#         measurements = measurements.filter(condition)

#     java_IMR = ec._sc._jvm.com.epidata.spark.analytics.IMR.get()
#     jdf = java_IMR.applyToDataFrame(measurements._pdf._jdf)
#     return DataFrame(jdf=jdf, sql_ctx=measurements._pdf.sql_ctx)


def identity(df, meas_names = []):
    if 'meas_method' not in df.columns:
        df.loc[:, 'meas_method'] = ""
    if 'meas_flag' not in df.columns:
        df.loc[:, 'meas_flag'] = ""
    return df


def outlier_detector(df_input, meas_names, column, method):
    """
    Identify outliers within a data frame column, using the specified method.

    Parameters
    ----------
    df : pandas DataFrame
        A DataFrame in which to detect outliers.
    column : string
        The name of the column within df in which to detect outliers.
        The column must contain numeric values. Missing values are ignored.
    method : string
        The name of the outlier detection method. Available methods include:
        'quartile'.

    Returns
    -------
    result : pandas DataFrame
        A DataFrame containing full copies of the rows within df which have
        outlier values. Some outlier methods may add a new column to the result
        to indicate the type of outlier.
    """
    if method == 'quartile':
        df = df_input[df_input[column].apply(
            lambda x: isinstance(x, (float, int, int, np.longlong, np.longdouble)))]
        if df.empty:
            return df
        q1 = df[column].quantile(q=0.25)
        q3 = df[column].quantile(q=0.75)
        iqr = q3 - q1  # Interquartile Range
        lif = q1 - 1.5 * iqr  # Lower Inner Fence
        lof = q1 - 3.0 * iqr  # Lower Outer Fence
        uif = q3 + 1.5 * iqr  # Upper Inner Fence
        uof = q3 + 3.0 * iqr  # Upper Outer Fence
        outliers = df[(df[column] < lif) | (df[column] > uif)]
        outliers.loc[:, 'meas_flag'] = list(map(
            lambda x: 'extreme' if x < lof or x > uof else 'mild',
            outliers[column]))
        if not outliers.empty:
            outliers.loc[:, 'meas_method'] = method
        return outliers

    raise ValueError('Unexpected outlier method: ' + repr(method))

def meas_statistics(df, meas_names, method="standard"):
    """
    Compute statistics on measurement values within a data frame, using the specified method.

    Parameters
    ----------
    df : pandas DataFrame
        A DataFrame in which to compute statistics.
    meas_names : list of strings
        The names of the measurements for which to perform statistics.
        The measurement values corresponding to the measurement names must contain numeric values.
    method : string
        The name of the statistics computation method. Available methods include:
        'standard'.

    Returns
    -------
    result : pandas DataFrame
        A DataFrame containing computed statistics for specified measurement names within df. Some methods may add new columns to the result with appropriate information.
    """

    def subgroup_statistics(row):
        row['start_time'] = np.min(row["ts"])
        row["stop_time"] = np.max(row["ts"])
        row["meas_summary_name"] = "statistics"
        count = np.nan
        mean = np.nan
        std = np.nan
        min = np.nan
        max = np.nan
        if not np.isnan(row["meas_value"].count()):
            count = int(row["meas_value"].count())
        if not np.isnan(row["meas_value"].mean()):
            mean = int(row["meas_value"].mean())
        if not np.isnan(row["meas_value"].std()):
            std = int(row["meas_value"].std())
        if not np.isnan(row["meas_value"].min()):
            min = int(row["meas_value"].min())
        if not np.isnan(row["meas_value"].max()):
            max = int(row["meas_value"].max())
        row["meas_summary_value"] = json.dumps(
            {
                'count': count,
                'mean': mean,
                'std': std,
                'min': min,
                'max': max})
        row["meas_summary_description"] = "descriptive statistics"
        return row

    if (method == "standard"):
        df_filtered = df[df['meas_value'].apply(
            lambda x: isinstance(x, (float, int, int, np.longlong, np.longdouble)))]
        df_grouped = df_filtered.loc[df_filtered["meas_name"].isin(meas_names)].groupby(
            ["company", "site", "station", "sensor"], as_index=False)
        df_summary = df_grouped.apply(subgroup_statistics).loc[:,
                                                               ["company",
                                                                "site",
                                                                "station",
                                                                "sensor",
                                                                "start_time",
                                                                "stop_time",
                                                                "event",
                                                                "meas_name",
                                                                "meas_summary_name",
                                                                "meas_summary_value",
                                                                "meas_summary_description"]].drop_duplicates()
    else:
        raise ValueError("Unsupported summary method: ", repr(method))

    return df_summary


def meas_statistics_automated_test(df, meas_names, method="standard"):
    """
    Compute statistics on measurement values within a data frame, using the specified method.

    Parameters
    ----------
    df : pandas DataFrame
        A DataFrame in which to compute statistics.
    meas_names : list of strings
        The names of the measurements for which to perform statistics.
        The measurement values corresponding to the measurement names must contain numeric values.
    method : string
        The name of the statistics computation method. Available methods include:
        'standard'.

    Returns
    -------
    result : pandas DataFrame
        A DataFrame containing computed statistics for specified measurement names within df. Some methods may add new columns to the result with appropriate information.
    """

    def subgroup_statistics(row):
        row['start_time'] = np.min(row["ts"])
        row["stop_time"] = np.max(row["ts"])
        row["meas_summary_name"] = "statistics"
        row["meas_summary_value"] = json.dumps(
            {
                'count': row["meas_value"].count(),
                'mean': row["meas_value"].mean(),
                'std': row["meas_value"].std(),
                'min': row["meas_value"].min(),
                'max': row["meas_value"].max()})
        row["meas_summary_description"] = "descriptive statistics"
        return row

    if (method == "standard"):
        df_filtered = df[df['meas_value'].apply(
            lambda x: isinstance(x, (float, long, int, np.int64, np.float64)))]
        df_grouped = df_filtered.loc[df_filtered["meas_name"].isin(meas_names)].groupby(
            ["company", "site", "device_group", "tester"], as_index=False)
        df_summary = df_grouped.apply(subgroup_statistics).loc[:,
                                                               ["company",
                                                                "site",
                                                                "device_group",
                                                                "tester",
                                                                "start_time",
                                                                "stop_time",
                                                                "device_name",
                                                                "test_name",
                                                                "meas_name",
                                                                "meas_summary_name",
                                                                "meas_summary_value",
                                                                "meas_summary_description"]].drop_duplicates()
    else:
        raise ValueError("Unsupported summary method: ", repr(method))

    return df_summary
