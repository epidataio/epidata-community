#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from data_frame import DataFrame
import pandas as pd
import math
import numbers
import copy
import numpy as np
import json


def IMR(measurements, meas_names=None):
    """
    Perform IMR analysis on a DataFrame of measurements. The measurements are
    grouped by the 'meas_name' field and IMR is performed on each group.

    Parameters
    ----------
    measurements : epidata DataFrame
        A DataFrame containing measurements, as returned by EpidataContext.query.
    meas_names : list of strings, or string or None, default None
        A list of measurement names on which to perform IMR, or a single
        measurement name on which to perform IMR. If None, all measurements will
        be analyzed.

    Returns
    -------
    result : epidata DataFrame
        A copy of the measurements DataFrame, with the IMR results appended as
        additional columns.
    """

    from context import ec

    local = not isinstance(measurements, DataFrame)
    if local:
        raise ValueError('Unsupported local measurements argument to IMR.')

    if isinstance(meas_names, basestring):
        # Filter a single string measurement name.
        measurements = measurements.filter(
            measurements.meas_name == meas_names)
    elif meas_names:
        # Build a composite filter for a list of measurement names.
        condition = (measurements.meas_name == meas_names[0])
        for name in meas_names[1:]:
            condition = (condition | (measurements.meas_name == name))
        measurements = measurements.filter(condition)

    java_IMR = ec._sc._jvm.com.epidata.spark.analytics.IMR.get()
    jdf = java_IMR.applyToDataFrame(measurements._pdf._jdf)
    return DataFrame(jdf=jdf, sql_ctx=measurements._pdf.sql_ctx)


def identity(df):
    if not 'meas_method' in df.columns:
        df.loc[:, 'meas_method'] = ""
    if not 'meas_flag' in df.columns:
        df.loc[:, 'meas_flag'] = ""
    return df


def outliers(df_input, column, method):
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
    df = df_input[df_input['meas_value'].apply(lambda x: isinstance(x, (float, long, int, np.int64, np.float64)))]
    if method == 'quartile':
        median = df[column].median()
        q1 = df[column].quantile(q=0.25)
        q3 = df[column].quantile(q=0.75)
        iqr = q3 - q1  # Interquartile Range
        lif = q1 - 1.5 * iqr  # Lower Inner Fence
        lof = q1 - 3.0 * iqr  # Lower Outer Fence
        uif = q3 + 1.5 * iqr  # Upper Inner Fence
        uof = q3 + 3.0 * iqr  # Upper Outer Fence
        outliers = df[(df[column] < lif) | (df[column] > uif)]
        outliers.loc[:, 'meas_flag'] = map(
            lambda x: 'extreme' if x < lof or x > uof else 'mild',
            outliers[column])
        if not outliers.empty:
            outliers.loc[:, 'meas_method'] = method
        return outliers

    raise ValueError('Unexpected outlier method: ' + repr(method))

def substitute(df, meas_names, method="rolling", size=3):
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
    for meas_name in meas_names:

        if (method == "rolling"):
            if ((size % 2 == 0) and (size != 0)):
                size += 1
            if df.loc[df["meas_name"] == meas_name].size > 0:
                indices = df.loc[df["meas_name"] == meas_name].index[df.loc[df["meas_name"]== meas_name]["meas_value"].apply(lambda x: not isinstance(x, basestring) and np.isnan(x))]
                substitutes = df.loc[df["meas_name"] == meas_name]["meas_value"].rolling(
                    window=size, min_periods=1, center=True).mean()

                df["meas_value"].fillna(substitutes, inplace=True)
                df.loc[indices, "meas_flag"] = "substituted"
                df.loc[indices, "meas_method"] = "rolling average"
        else:
            raise ValueError("Unsupported substitution method: ", repr(method))

    return df


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
        df_filtered = df[df['meas_value'].apply(lambda x: isinstance(x, (float, long, int, np.int64, np.float64)))]
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
