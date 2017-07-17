#
# Copyright (c) 2015-2017 EpiData, Inc.
#

import numpy


class Waveform(object):

    """
    An array of samples comprising a waveform.

    Parameters
    ----------
    start_time : datetime
        The time when sampling began.
    sampling_rate : float64
        The sampling rate.
    samples: numpy.array of float64
        The samples.
    """

    def __init__(self, start_time, sampling_rate, samples):
        self.start_time = start_time
        self.sampling_rate = sampling_rate
        self.samples = samples

    def __repr__(self):
        return 'Waveform({0}, {1}, {2})'.format(repr(self.start_time),
                                                repr(self.sampling_rate),
                                                repr(self.samples))

    def __str__(self):
        return 'W {0} {1} {2}'.format(self.start_time.isoformat(),
                                      repr(self.sampling_rate),
                                      numpy.array_str(self.samples))

    def __eq__(self, other):
        return self.start_time == other.start_time and \
            self.sampling_rate == other.sampling_rate and \
            numpy.array_equal(self.samples, other.samples)

    def __ne__(self, other):
        return not self.__eq__(other)
