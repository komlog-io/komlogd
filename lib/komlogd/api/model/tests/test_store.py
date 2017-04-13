import unittest
import decimal
import pandas as pd
import datetime
import uuid
from komlogd.api.model import store
from komlogd.api.protocol.model.types import Datasource, Datapoint
from komlogd.api.protocol.model.transfer_methods import DataRequirements

class ApiModelStoreTest(unittest.TestCase):

    def test_store_creation_success(self):
        ''' store object creation without parameters should succeed '''
        ms=store.MetricsStore(owner='owner')
        self.assertTrue(isinstance(ms, store.MetricsStore))
        self.assertEqual(ms.owner, 'owner')
        self.assertEqual(ms.maintenance_exec_delta.seconds, 300)
        self.assertEqual(ms.default_reqs.past_delta.seconds, 600)
        self.assertEqual(ms.default_reqs.past_count, 2)
        self.assertEqual(ms._metric_reqs, {})
        self.assertEqual(ms._series, {})

    def test_store_creation_with_parameters_success(self):
        ''' store object creation with parameters should succeed '''
        purge_exec_delta=pd.Timedelta('1 d')
        reqs=DataRequirements(past_delta=pd.Timedelta('24 h'))
        ms=store.MetricsStore(owner='owner', maintenance_exec_delta=purge_exec_delta, data_delta=reqs)
        self.assertTrue(isinstance(ms, store.MetricsStore))
        self.assertTrue(isinstance(ms._series, dict))
        self.assertEqual(ms.maintenance_exec_delta.seconds, 0)
        self.assertEqual(ms.maintenance_exec_delta.days, 1)
        self.assertEqual(ms.default_reqs.past_delta.seconds, 0)
        self.assertEqual(ms.default_reqs.past_delta.days, 1)
        self.assertEqual(ms._metric_reqs, {})

    def test_store_new_datasource_metric_content_success(self):
        ''' if we store a new ds metric in the store, the series should be created with the new value and ts '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='ds.uri')
        content='datasource content with 1 value.'
        ts=pd.Timestamp('now',tz='utc')
        ms.store(metric=metric, ts=ts, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(serie.index[0],ts)
        self.assertEqual(serie[0],content)
        self.assertEqual(ms._metric_reqs, {})

    def test_store_new_datasource_metric_content_failure_invalid_ds_content(self):
        ''' storing a datasource metric with invalid content should fail '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='ds.uri')
        content=['a','list','4','example']
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            ms.store(metric=metric, ts=ts, content=content)
        self.assertEqual(str(cm.exception),'content is not a string')

    def test_store_new_datapoint_metric_content_success(self):
        ''' if we store a new dp metric in the store, the series should be created with the new value and ts '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts=pd.Timestamp('now',tz='utc')
        ms.store(metric=metric, ts=ts, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(serie.index[0],ts)
        self.assertEqual(serie[0],float(content))
        self.assertEqual(ms._metric_reqs, {})

    def test_store_new_datapoint_metric_content_failure_invalid_dp_content(self):
        ''' storing a datapoint metric with invalid content should fail '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content='content'
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            ms.store(metric=metric, ts=ts, content=content)
        self.assertEqual(str(cm.exception),'datapoint value not valid')

    def test_store_new_metric_content_failure_ts_has_no_timezone(self):
        ''' Timestamps without timezone information are not valid '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts=pd.Timestamp('now')
        with self.assertRaises(TypeError) as cm:
            ms.store(metric=metric, ts=ts, content=content)
        self.assertEqual(str(cm.exception),'timezone is required')

    def test_store_already_existing_datapoint_metric_content_success(self):
        ''' if we store an already existing dp metric in the store, contents should be appended to the existing metric '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35')
        ts=pd.Timestamp('now',tz='utc')
        ms.store(metric=metric, ts=ts, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(serie.index[0],ts)
        self.assertEqual(serie[0],int(content))
        content2=decimal.Decimal('36.221')
        ts2=pd.Timestamp('now',tz='utc')
        ms.store(metric=metric, ts=ts2, content=content2)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),2)
        self.assertEqual(serie.index[1],ts2)
        self.assertEqual(serie[1],float(content2))

    def test_overwrite_existing_datapoint_content(self):
        ''' if we store an already existing ts, the content will be replaced '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts=pd.Timestamp('now',tz='utc')
        ms.store(metric=metric, ts=ts, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),1)
        self.assertEqual(serie.index[0],ts)
        self.assertEqual(serie[0],float(content))
        content2=decimal.Decimal('36.221')
        ts2=ts
        ms.store(metric=metric, ts=ts2, content=content2)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),1)
        self.assertEqual(serie.index[0],ts)
        self.assertEqual(serie[0],float(content2))

    def test_sort_index_if_ts_less_than_last_in_index(self):
        ''' if we insert a row which a ts older than the last in the series, the index should be sorted '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts1=pd.Timestamp('2016-07-30',tz='utc')
        ts2=pd.Timestamp('2016-07-31',tz='utc')
        ts3=pd.Timestamp('2016-07-29',tz='utc')
        ms.store(metric=metric, ts=ts1, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),1)
        self.assertEqual(serie.index[0],ts1)
        ms.store(metric=metric, ts=ts2, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),2)
        self.assertEqual(serie.index[0],ts1)
        self.assertEqual(serie.index[1],ts2)
        ms.store(metric=metric, ts=ts3, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),3)
        self.assertEqual(serie.index[0],ts3)
        self.assertEqual(serie.index[1],ts1)
        self.assertEqual(serie.index[2],ts2)

    def test_purge_should_delete_contents_older_than_set_limit(self):
        ''' purge should delete contents older than limit set by param data_delta. '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts3=pd.Timestamp('now',tz='utc')
        ts2=ts3 - pd.Timedelta('1s')
        ts1=ts2 - pd.Timedelta('1s')
        ms.store(metric=metric, ts=ts1, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),1)
        self.assertEqual(serie.index[0],ts1)
        ms.store(metric=metric, ts=ts2, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),2)
        self.assertEqual(serie.index[0],ts1)
        self.assertEqual(serie.index[1],ts2)
        ms.store(metric=metric, ts=ts3, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),3)
        self.assertEqual(serie.index[0],ts1)
        self.assertEqual(serie.index[1],ts2)
        self.assertEqual(serie.index[2],ts3)
        ms.purge()
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),3) #DataRequirements by default are 2 samples or 10 min of data
        ms.default_reqs=DataRequirements(past_delta=pd.Timedelta('1 us'))
        ms.purge()
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),0)

    def test_purge_should_not_execute_because_last_purge_exec(self):
        ''' purge should not execute if last_purge_exec + purge_exec_delta is a time in the future '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts1=pd.Timestamp('1990-03-01',tz='utc')
        ts2=pd.Timestamp('1991-03-01',tz='utc')
        ts3=pd.Timestamp('1992-03-01',tz='utc')
        ms.store(metric=metric, ts=ts1, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),1)
        self.assertEqual(serie.index[0],ts1)
        ms.store(metric=metric, ts=ts2, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),2)
        self.assertEqual(serie.index[0],ts1)
        self.assertEqual(serie.index[1],ts2)
        ms.store(metric=metric, ts=ts3, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),3)
        self.assertEqual(serie.index[0],ts1)
        self.assertEqual(serie.index[1],ts2)
        self.assertEqual(serie.index[2],ts3)
        ms.purge()
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),2) #DataRequirements by default are 2 samples or 10 min of data
        ms.default_reqs=DataRequirements(past_delta=pd.Timedelta('1 us'))
        ms.purge()
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),0)

    def test_purge_should_take_specific_metric_delta_if_it_exists(self):
        ''' purge should take specific metric delta if it exists '''
        reqs=DataRequirements(past_delta=pd.Timedelta('24h'))
        ms=store.MetricsStore(owner='owner',maintenance_exec_delta=pd.Timedelta('1 us'),data_delta=reqs)
        metric=Datapoint(uri='dp.uri')
        content=decimal.Decimal('35.221')
        ts1=pd.Timestamp('1990-03-01',tz='utc')
        ts2=pd.Timestamp('1991-03-01',tz='utc')
        ts3=pd.Timestamp('1992-03-01',tz='utc')
        ms.store(metric=metric, ts=ts1, content=content)
        ms.store(metric=metric, ts=ts2, content=content)
        ms.store(metric=metric, ts=ts3, content=content)
        global_metric=Datapoint(uri='dp.uri2')
        ms.store(metric=global_metric, ts=ts1, content=content)
        ms.store(metric=global_metric, ts=ts2, content=content)
        ms.store(metric=global_metric, ts=ts3, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),3)
        serie = ms.get_serie(global_metric)
        self.assertEqual(len(serie),3)
        specific_delta=DataRequirements(past_delta=pd.Timestamp('now',tz='utc')-pd.Timestamp('1991-02-28',tz='utc'))
        self.assertTrue(ms.add_metric_data_reqs(metric, specific_delta))
        ms.purge()
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie),2)
        serie = ms.get_serie(global_metric)
        self.assertEqual(len(serie),0)

    def test_add_metric_data_reqs_success_non_existent_metric_previously(self):
        ''' add_metric_data_reqs should succeed seting the reqs to a non previously existent metric '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        delta=pd.Timedelta('48 h')
        reqs = DataRequirements(past_delta=delta)
        self.assertTrue(ms.add_metric_data_reqs(metric=metric, reqs=reqs))
        d_stablished=ms.get_metric_data_reqs(metric)
        self.assertEqual(d_stablished.past_delta, delta)

    def test_add_metric_data_reqs_success_metric_existed_previously_past_delta_set(self):
        ''' add_metric_data_reqs should succeed seting the data delta to a previously existent metric if delta is greater than the existent one '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        delta=pd.Timedelta('48 h')
        reqs = DataRequirements(past_delta=delta)
        self.assertTrue(ms.add_metric_data_reqs(metric=metric, reqs=reqs))
        d_stablished=ms.get_metric_data_reqs(metric)
        self.assertEqual(d_stablished.past_delta, delta)
        new_delta=pd.Timedelta('96 h')
        reqs = DataRequirements(past_delta=new_delta)
        self.assertTrue(ms.add_metric_data_reqs(metric=metric, reqs=reqs))
        d_stablished=ms.get_metric_data_reqs(metric)
        self.assertEqual(d_stablished.past_delta, new_delta)

    def test_add_metric_data_delta_success_metric_existed_previously_past_delta_smaller(self):
        ''' add_metric_data_reqs should succeed seting the data delta to a previously existent metric if delta is smaller than the existent one '''
        ms=store.MetricsStore(owner='owner')
        metric=Datapoint(uri='dp.uri')
        delta=pd.Timedelta('48 h')
        reqs = DataRequirements(past_delta=delta)
        self.assertTrue(ms.add_metric_data_reqs(metric=metric, reqs=reqs))
        d_stablished=ms.get_metric_data_reqs(metric)
        self.assertEqual(d_stablished.past_delta, delta)
        new_delta=pd.Timedelta('24 h')
        reqs = DataRequirements(past_delta=new_delta)
        self.assertTrue(ms.add_metric_data_reqs(metric=metric, reqs=reqs))
        d_stablished=ms.get_metric_data_reqs(metric)
        self.assertEqual(d_stablished.past_delta, delta)

    def test_get_metric_data_reqs_success_no_delta_found(self):
        ''' get_metric_data_reqs should return None if metric has no delta set '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='ds.uri')
        self.assertIsNone(ms.get_metric_data_reqs(metric))

    def test_get_metric_data_reqs_success_delta_found(self):
        ''' get_metric_data_reqs should return the delta set if metric has it '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='ds.uri')
        delta=pd.Timedelta('48 h')
        reqs = DataRequirements(past_delta=delta)
        self.assertTrue(ms.add_metric_data_reqs(metric=metric, reqs=reqs))
        d_stablished=ms.get_metric_data_reqs(metric)
        self.assertEqual(d_stablished.past_delta, delta)

    def test_get_serie_metric_not_found(self):
        ''' get_serie should return an empty serie if metric is not found '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='ds.uri')
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie), 0)

    def test_get_serie_full_data(self):
        ''' get_serie should return a copy of the full serie if no interval or count parameter is passed'''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='owner:ds.uri')
        tss = [pd.Timestamp(value,tz='utc') for value in [ns*1000 for ns in range(1,1000)]]
        for ts in tss:
            content = str(ts)
            ms.store(metric=metric, ts=ts, content=content)
        serie = ms.get_serie(metric)
        self.assertEqual(len(serie), len(ms._series[metric.uri]))

    def test_get_serie_data_interval(self):
        ''' get_serie should return a copy of the interval passed '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='owner:ds.uri')
        tss = [pd.Timestamp(value,tz='utc') for value in [ns*1000 for ns in range(1,1000)]]
        for ts in tss:
            content = str(ts)
            ms.store(metric=metric, ts=ts, content=content)
        ets=pd.Timestamp(500000, tz='utc')
        its=pd.Timestamp(1000, tz='utc')
        serie = ms.get_serie(metric, ets=ets, its=its)
        self.assertEqual(len(serie), 500)
        self.assertEqual(serie.index[0],its)
        self.assertEqual(serie.ix[0],str(its))
        self.assertEqual(serie.index[-1],ets)
        self.assertEqual(serie.ix[-1],str(ets))

    def test_get_serie_data_count(self):
        ''' get_serie should return a copy of the serie with as much rows as requested '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='owner:ds.uri')
        tss = [pd.Timestamp(value,tz='utc') for value in [ns*1000 for ns in range(1,1000)]]
        for ts in tss:
            content = str(ts)
            ms.store(metric=metric, ts=ts, content=content)
        ets=pd.Timestamp(500000, tz='utc')
        count=10
        serie = ms.get_serie(metric, ets=ets, count=count)
        self.assertEqual(len(serie), 10)
        self.assertEqual(serie.index[-1],ets)
        self.assertEqual(serie.ix[-1],str(ets))

    def test_get_serie_data_last(self):
        ''' get_serie should return a copy of the serie with the last row with ts older than the requested one '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='owner:ds.uri')
        tss = [pd.Timestamp(value,tz='utc') for value in [ns*1000 for ns in range(1,1000)]]
        for ts in tss:
            content = str(ts)
            ms.store(metric=metric, ts=ts, content=content)
        ets_req=pd.Timestamp(500500, tz='utc')
        ets_data=pd.Timestamp(500000, tz='utc')
        serie = ms.get_serie(metric, ets=ets_req)
        self.assertEqual(len(serie), 1)
        self.assertEqual(serie.index[-1],ets_data)
        self.assertEqual(serie.ix[-1],str(ets_data))

    def test_get_serie_modify_data_does_not_modify_store(self):
        ''' get_serie should return a copy of the serie '''
        ms=store.MetricsStore(owner='owner')
        metric=Datasource(uri='owner:ds.uri')
        tss = [pd.Timestamp(value,tz='utc') for value in [ns*1000 for ns in range(1,1000)]]
        for ts in tss:
            content = str(ts)
            ms.store(metric=metric, ts=ts, content=content)
        ets=pd.Timestamp(500000, tz='utc')
        its=pd.Timestamp(1000, tz='utc')
        serie = ms.get_serie(metric, ets=ets, its=its)
        self.assertEqual(len(serie), 500)
        self.assertEqual(serie.index[0],its)
        self.assertEqual(serie.ix[0],str(its))
        self.assertEqual(serie.index[-1],ets)
        self.assertEqual(serie.ix[-1],str(ets))
        for i in range(0,500):
            serie[i] = str(serie[i])+'modified'
        serie2 = ms.get_serie(metric, ets=ets, its=its)
        for i in range(0,500):
            self.assertEqual(serie[i],str(pd.Timestamp((i+1)*1000, tz='utc'))+'modified')
            self.assertEqual(serie2[i],str(pd.Timestamp((i+1)*1000, tz='utc')))

