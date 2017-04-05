import pandas as pd

class DataRequirements:
    def __init__(self, past_delta=None, past_count=None):
        self.past_delta = past_delta
        self.past_count = past_count

    @property
    def past_delta(self):
        return self._past_delta

    @past_delta.setter
    def past_delta(self, value):
        if isinstance(value, pd.Timedelta) or value is None:
            self._past_delta = value
        else:
            raise TypeError('Invalid past_delta parameter')

    @property
    def past_count(self):
        return self._past_count

    @past_count.setter
    def past_count(self, value):
        if (isinstance(value, int) and value >= 0) or value is None:
            self._past_count = value
        else:
            raise TypeError('Invalid past_count parameter')


