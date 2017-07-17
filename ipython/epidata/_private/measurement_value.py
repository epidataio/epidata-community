#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from datetime import datetime
from epidata_common.data_types import Waveform
import numbers
import numpy
from pyspark.sql.types import UserDefinedType, StructField, StructType, StringType, BinaryType, DoubleType, \
    IntegerType
import struct


DOUBLE_SIZE = 8
LONG_SIZE = 8


class MeasurementValueUDT(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return StructType([
            StructField('double', DoubleType(), True),
            StructField('int', IntegerType(), True),
            StructField('string', StringType(), True),
            StructField('binary', BinaryType(), True)])

    @classmethod
    def module(cls):
        return 'epidata._private.measurement_value'

    @classmethod
    def scalaUDT(cls):
        return 'com.epidata.spark.MeasurementValueUDT'

    def serialize(self, obj):
        raise NotImplementedError(
            'MeasurementValueUDT.serialize not implemented.')

    def deserialize(self, datum):
        assert len(
            datum) == 4, "MeasurementValueUDT.deserialize given row with length %d but requires 4" % len(datum)
        if datum[0] is not None:
            return datum[0]
        elif datum[1] is not None:
            return datum[1]
        if datum[2] is not None:
            return datum[2]
        if datum[3] is not None:

            # Element 3 contains binary data.
            binary = datum[3]

            if binary[0] == 16:
                # Unpack a numpy array of doubles.
                dt = numpy.dtype(float)
                dt = dt.newbyteorder('>')
                return numpy.frombuffer(buffer(binary, 1), dtype=dt)

            elif binary[0] == 32:
                # Unpack a waveform.
                (start_time_long, sampling_rate) = struct.unpack_from(
                    '!qd', buffer(binary, 1))
                start_time = datetime.fromtimestamp(start_time_long / 1000.0)
                dt = numpy.dtype(float)
                dt = dt.newbyteorder('>')
                samples = numpy.frombuffer(
                    buffer(binary, 1 + LONG_SIZE + DOUBLE_SIZE), dtype=dt)
                return Waveform(start_time, sampling_rate, samples)

            else:
                return binary

        else:
            return None

    def simpleString(self):
        return 'measurement_value'
