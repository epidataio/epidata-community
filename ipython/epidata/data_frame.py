#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from epidata._private.measurement_value import MeasurementValueUDT
from numbers import Number
import numpy
import pandas
from pyspark.sql.dataframe import DataFrame as PysparkDataFrame
from pyspark.sql.types import DoubleType, IntegerType


class DataFrame:

    """
    Measurement data in a tabular format. The data itself is stored
    distributively rather than on a local computer. Distributed filtering and
    distributed computations may be performed on a DataFrame, producing a new
    DataFrame. In addition, the functions toPandas() and retrieve() can be used
    to load a local copy of the data. A DataFrame is typically created by
    calling EpidataContext.query to request measurements from Epidata servers.
    """

    def __init__(self, jdf=None, sql_ctx=None, pdf=None):
        self._pdf = pdf or PysparkDataFrame(jdf, sql_ctx)

    def __getattr__(self, name):
        return self._pdf.__getattr__(name)

    def __getitem__(self, item):
        ret = self._pdf.__getitem__(item)
        if isinstance(ret, PysparkDataFrame):
            ret = DataFrame(pdf=ret)
        return ret

    def __repr__(self):
        return self._pdf.__repr__()

    def cache(self):
        """
        Request that the contents of this DataFrame be saved in memory on the
        remote servers. The DataFrame will be saved in memory the next time it
        is accessed. On subsequent access, the data within the DataFrame will
        be read from in memory storage.

        Returns
        -------
        result : epidata DataFrame
            The DataFrame itself.
        """
        self._pdf.cache()
        return self

    def count(self):
        """
        Count the number of rows in the DataFrame.

        Returns
        -------
        result : long
            The number of rows in the DataFrame.
        """
        return self._pdf.count()

    def describe(self):
        """
        Calculate summary statistics for numeric fields.

        Returns
        -------
        result : pandas DataFrame
            A pandas DataFrame containing the count, mean, std, min, and max
            values for numeric fields.
        """

        # Find numeric fields.
        expected_numeric_types = [
            DoubleType(),
            IntegerType(),
            MeasurementValueUDT()]
        numeric_fields = filter(lambda x: x.dataType in expected_numeric_types,
                                self.schema.fields)
        numeric_field_names = map(lambda x: x.name, numeric_fields)
        selected = self[numeric_field_names]

        # Calculate statistics for the numeric fields.
        fold_zero = [self._describe_init() for i in numeric_field_names]
        stats = selected._pdf.rdd.map(
            self._describe_mapper).fold(
            fold_zero, self._describe_reducer)
        labeled_stats = dict(zip(numeric_field_names, stats))
        stats_frame = pandas.DataFrame.from_dict(
            labeled_stats)[numeric_field_names]

        # Add additional statistics.
        stats_sum = stats_frame.loc['sum', :].astype('float64')
        stats_sumsq = stats_frame.loc['sumsq', :].astype('float64')
        stats_count = stats_frame.loc['count', :].astype('float64')
        stats_mean = stats_sum / stats_count
        stats_mean_sum_sq = stats_sumsq / stats_count
        stats_variance = (stats_mean_sum_sq - stats_mean **
                          2) * stats_count / (stats_count - 1)
        stats_frame.loc['mean', :] = stats_mean
        stats_frame.loc['std', :] = numpy.sqrt(stats_variance)

        # Reorder the rows, and return.
        return stats_frame.loc[['count', 'mean', 'std', 'min', 'max'], :]

    def describe_by_group(self, group_col, value_cols):
        """
        Calculate summary statistics for groups of rows, based on the parameters.

        Parameters
        ----------
        group_col : string
            The name of the column for grouping data when calculating summary
            statistics.
        value_cols : list of strings
            The names of columns for which summary statistics are to be
            calculated.

        Returns
        -------
        result : pandas DataFrame
            A pandas DataFrame containing the count, mean, std, min, and max
            values for the specified groups and fields.
        """

        # Select the requested fields.
        select_cols = [group_col] + value_cols
        selected = self[select_cols]

        # Group by the group col and calculate statistics.
        stats = dict(selected._pdf.rdd.map(
            lambda row: (
                row.asDict()[group_col],
                row)).mapValues(
            self._describe_mapper).reduceByKey(
                self._describe_reducer).collect())

        # Now combine the group names with the value cols to create new
        # statistics column labels.
        ordered_group_names = sorted(stats.keys())
        ordered_stats_fields = []
        labeled_stats = {}
        for group in ordered_group_names:
            for (key, key_stats) in zip(select_cols, stats[group]):
                if key == group_col:
                    # Don't report stats for the group col.
                    continue
                field_name = group + '.' + key
                ordered_stats_fields.append(field_name)
                labeled_stats[field_name] = key_stats
        stats_frame = pandas.DataFrame.from_dict(
            labeled_stats)[ordered_stats_fields]

        # The row index of the returned data.
        return_index = ['count', 'mean', 'std', 'min', 'max']

        if stats_frame.empty:
            # If there is no data, return an empty pandas DataFrame.
            return pandas.DataFrame(index=return_index, dtype='float64')

        # Add additional statistics.
        stats_sum = stats_frame.loc['sum', :].astype('float64')
        stats_sumsq = stats_frame.loc['sumsq', :].astype('float64')
        stats_count = stats_frame.loc['count', :].astype('float64')
        stats_mean = stats_sum / stats_count
        stats_mean_sum_sq = stats_sumsq / stats_count
        stats_variance = (stats_mean_sum_sq - stats_mean **
                          2) * stats_count / (stats_count - 1)
        stats_frame.loc['mean', :] = stats_mean
        stats_frame.loc['std', :] = numpy.sqrt(stats_variance)

        # Reorder the fields and return.
        return stats_frame.loc[return_index, :]

    def distinct(self):
        """
        Filter out duplicate rows.

        Returns
        -------
        result : epidata DataFrame
            A copy of this DataFrame, but with any duplicate rows excluded.
        """
        return DataFrame(pdf=self._pdf.distinct())

    def filter(self, condition):
        """
        Filter rows using a boolean condition.

        Parameters
        ----------
        condition : Column expression or string
            A condition describing matching rows (rows preserved in the output),
            expressed as either a boolean expression of one or more Column
            objects or a string representing a SQL expression.

        Returns
        -------
        result : epidata DataFrame
            A new DataFrame, containing all rows matching the condition.
        """
        return DataFrame(pdf=self._pdf.filter(condition))

    def get_device_data(self, device_name):
        """
        Get all rows for the specified device as a pandas DataFrame.

        Parameters
        ----------
        device_name : string
            The name of the device for which data will be returned.

        Returns
        -------
        result : pandas DataFrame
            A pandas DataFrame containing all data for the specified device. The
            rows will be sorted by timestamp if a 'ts' field exists.
        """
        if not self._has_field('device_name'):
            raise KeyError(
                'DataFrame does not contain a \'device_name\' field.')
        filtered = self.filter(self.device_name == device_name).toPandas()
        return filtered.sort('ts') if self._has_field('ts') else filtered

    def get_meas_data(self, meas_name):
        """
        Get all rows for the specified measurement name as a pandas DataFrame.

        Parameters
        ----------
        meas_name : string
            The name of the measurement for which data will be returned.

        Returns
        -------
        result : pandas DataFrame
            A pandas DataFrame containing all data for the specified measurement.
            The rows will be sorted by timestamp if a 'ts' field exists.
        """
        if not self._has_field('meas_name'):
            raise KeyError('DataFrame does not contain a \'meas_name\' field.')
        filtered = self.filter(self.meas_name == meas_name).toPandas()
        return filtered.sort('ts') if self._has_field('ts') else filtered

    def head(self, n=None):
        """
        Get some rows from the DataFrame.

        Parameters
        ----------
        n : int or None, default None
            The number of rows to return. By default a single row is returned.

        Returns
        -------
        result : list of Rows or a single Row
            A list containing the requested number of Row objects from the
            DataFrame. If 'n' is None, a single Row is returned (not within a
            list).
        """
        return self._pdf.head(n)

    def limit(self, n):
        """
        Create a DataFrame containing a limited number of rows from this
        DataFrame.

        Parameters
        ----------
        n : int
            The number of rows to include in the new DataFrame.

        Returns
        -------
        result : epidata DataFrame
            A new DataFrame, containing no more than n rows from this DataFrame.
        """
        return DataFrame(pdf=self._pdf.limit(n))

    def retrieve(self):
        """
        Get the rows from this DataFrame.

        Returns
        -------
        result : list of Rows
            Returns the Rows of this DataFrame as a list in local memory.
        """
        return self._pdf.collect()

    @property
    def schema(self):
        """
        Get the schema for this DataFrame.

        Returns
        -------
        result : StructType
            The Dataframe's schema.
        """
        return self._pdf.schema

    def select(self, *cols):
        """
        Select columns from the DataFrame.

        Parameters
        ----------
        cols : Column or string (or several columns or strings)
            One or more data frame columns, or one or more data frame column
            names, to select.

        Returns
        -------
        result : DataFrame
            A new DataFrame containing only the selected columns.
        """
        return DataFrame(pdf=self._pdf.select(*cols))

    def sort(self, *cols):
        """
        Sort rows from the DataFrame.

        Parameters
        ----------
        cols : string, Column or SortOrder, or several string, column, or SortOrders
            One or more columns (or column names) to sort by. And for SortOrder,
            the order in which to sort them.

        Returns
        -------
        result : DataFrame
            A new DataFrame where the rows are reordered to match the specified
            sort order.
        """
        return DataFrame(pdf=self._pdf.sort(*cols))

    def show(self, n=20):
        """
        Print some rows from the DataFrame.

        Parameters
        ----------
        n : int, default 20
            The number of rows to print.
        """
        print(self.limit(n).toPandas())

    def toPandas(self):
        """
        Retrieve all rows as a local pandas DataFrame.

        Returns
        -------
        output : pandas DataFrame
            A pandas DataFrame containing the rows in this epidata DataFrame.
        """

        def fixup_rows(row):

            def fixup_value(val):
                # If val is not a tuple, return it.
                if not isinstance(val, tuple):
                    return val
                # If val is a tuple, it is assumed to be a MeasurementValue, a
                # type union, and deserialized.
                return MeasurementValueUDT().deserialize(val)

            return map(fixup_value, row)

        rows = self._pdf.collect()
        fixed_rows = map(fixup_rows, rows)
        return pandas.DataFrame.from_records(
            fixed_rows,
            columns=self._pdf.columns)

    def union(self, other):
        """
        Combine two DataFrames.

        Parameters
        ----------
        other : DataFrame
            Another DataFrame, which will be concatenated with this DataFrame
            in the result. The 'other' DataFrame must have the same schema as
            this DataFrame.

        Returns
        -------
        output : DataFrame
            A new DataFrame combining the rows from this DataFrame and 'other'.
        """
        return DataFrame(pdf=self._pdf.unionAll(other._pdf))

    @staticmethod
    def _describe_init():
        return {'count': 0, 'max': None, 'min': None, 'sum': 0.0, 'sumsq': 0.0}

    @staticmethod
    def _describe_mapper(row):
        # Map a row to a list of aggregate values describing statistical properties
        # of its fields.
        ret = []
        for x in row:
            # All tuples are interpreted as the union type MeasurementValue. Users
            # are not expected to create tuple columns that are not
            # MeasurementValues.
            if isinstance(x, tuple):
                # Find the value within the union tuple.
                new_x = None
                for y in x:
                    if y is not None:
                        new_x = y
                        break
                x = new_x
            if x is None:
                # Exclude NULL values from aggregates.
                ret.append(DataFrame._describe_init())
            elif isinstance(x, Number):
                # Statistics are computed for numeric values.
                ret.append(
                    {'count': 1, 'max': x, 'min': x, 'sum': x, 'sumsq': x**2})
            else:
                # Non numeric values are counted, but aggregate fields are set
                # to NaN. If combined with numeric values, the resulting
                # aggregate fields will be NaN as well.
                nan = float('nan')
                ret.append({'count': 1,
                            'max': nan,
                            'min': nan,
                            'sum': nan,
                            'sumsq': nan})
        return ret

    @staticmethod
    def _describe_reducer(row_x, row_y):
        # Combine aggregate fields for two rows.
        for (x, y) in zip(row_x, row_y):
            x['count'] += y['count']
            if x['max'] is None:
                x['max'] = y['max']
            elif y['max'] is not None:
                x['max'] = numpy.max([x['max'], y['max']])
            if x['min'] is None:
                x['min'] = y['min']
            elif y['min'] is not None:
                x['min'] = numpy.min([x['min'], y['min']])
            x['sum'] += y['sum']
            x['sumsq'] += y['sumsq']
        return row_x

    def _has_field(self, field_name):
        return field_name in [x.name for x in self.schema.fields]
