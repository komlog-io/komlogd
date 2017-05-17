from komlogd.api import exceptions
from komlogd.api.protocol.model.types import Metric

class Schedule:
    def __new__(cls, *args, **kwargs):
        if cls is Schedule:
            raise TypeError("Schedule base class may not be instantiated")
        return object.__new__(cls)

    def __init__(self, exec_on_load):
        self.exec_on_load = exec_on_load

    @property
    def exec_on_load(self):
        return self._exec_on_load

    @exec_on_load.setter
    def exec_on_load(self, value):
        self._exec_on_load = bool(value)

class DummySchedule(Schedule):
    def __init__(self, exec_on_load=False):
        super().__init__(exec_on_load=exec_on_load)

class OnUpdateSchedule(Schedule):
    def __init__(self, metrics=None, exec_on_load=False):
        self.metrics = metrics
        super().__init__(exec_on_load=exec_on_load)

    @property
    def metrics(self):
        return self._metrics

    @metrics.setter
    def metrics(self, value):
        if value is None:
            self._metrics = []
        elif isinstance(value,list):
            for m in value:
                if not isinstance(m, Metric):
                    raise exceptions.BadParametersException('Invalid metric: '+str(m))
            else:
                self._metrics = value
        else:
            raise exceptions.BadParametersException('"metrics" attribute must be a list')

class CronSchedule(Schedule):
    def __init__(self, minute='*', hour='*', month='*', dow='*', dom='*', exec_on_load=False):
        super().__init__(exec_on_load=exec_on_load)
        self._schedule={}
        self.minute = minute
        self.hour = hour
        self.month = month
        self.dow = dow
        self.dom = dom

    @property
    def minute(self):
        return self._minute

    @minute.setter
    def minute(self, value):
        sched_entry = self._process_var(value, 59, 0)
        if sched_entry:
            self._schedule['minute']=sched_entry
            self._minute = value
        else:
            raise exceptions.BadParametersException('Invalid minute value: '+str(value))

    @property
    def hour(self):
        return self._hour

    @hour.setter
    def hour(self, value):
        sched_entry = self._process_var(value, 23, 0)
        if sched_entry:
            self._schedule['hour']=sched_entry
            self._hour = value
        else:
            raise exceptions.BadParametersException('Invalid hour value: '+str(value))

    @property
    def month(self):
        return self._month

    @month.setter
    def month(self, value):
        sched_entry = self._process_var(value, 12, 1)
        if sched_entry:
            self._schedule['month']=sched_entry
            self._month = value
        else:
            raise exceptions.BadParametersException('Invalid month value: '+str(value))

    @property
    def dow(self):
        return self._dow

    @dow.setter
    def dow(self, value):
        sched_entry = self._process_var(value, 6, 0)
        if sched_entry:
            self._schedule['dow']=sched_entry
            self._dow = value
        else:
            raise exceptions.BadParametersException('Invalid dow value: '+str(value))

    @property
    def dom(self):
        return self._dom

    @dom.setter
    def dom(self, value):
        sched_entry = self._process_var(value, 31, 1)
        if sched_entry:
            self._schedule['dom']=sched_entry
            self._dom = value
        else:
            raise exceptions.BadParametersException('Invalid dom value: '+str(value))

    def _process_var(self, value, max_value, min_value):
        processed_entry=[]
        in_range=range(min_value,max_value+1)
        try:
            int_v=int(value)
            processed_entry.append(int_v) if (int_v<=max_value and int_v>=min_value) else None
        except Exception:
            if value=='*':
                for i in range(min_value,max_value+1):
                    processed_entry.append(i)
            elif len(value.split(','))>1:
                for group in value.split(','):
                    result=self._process_var(group,max_value,min_value)
                    for value in result:
                        processed_entry.append(value)
            elif len(value.split('-'))>1:
                r=value.split('-')
                r_min=int(r[0])
                r_max=int(r[1])
                for i in range(r_min,r_max+1):
                    if i in in_range:
                        processed_entry.append(i) 
            elif len(value.split('/'))>1:
                num,den=value.split('/')
                if num=='*':
                    num_list=in_range
                else:
                    num_int=int(num)
                    num_list=[num_int]
                if den=='*':
                    for i in num_list:
                        if i in in_range:
                            processed_entry.append(i)
                else:
                    den_int=int(den)
                    tmp_list=[]
                    for i in num_list:
                        if i%den_int==0:
                            tmp_list.append(i)
                    for j in tmp_list:
                        processed_entry.append(j)
        result_list=list(set(processed_entry))
        return result_list

    def meets(self, t):
        minute = t.tm_min
        hour = t.tm_hour
        month = t.tm_mon
        dow = t.tm_wday
        dom = t.tm_mday
        if (minute in self._schedule['minute'] and
           hour in self._schedule['hour'] and
           month in self._schedule['month'] and
           dow in self._schedule['dow'] and
           dom in self._schedule['dom']):
            return True
        return False

