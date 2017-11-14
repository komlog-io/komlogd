'''

Datastores

'''

import asyncio
import time
import decimal
import pandas as pd
from komlogd.api.common import exceptions, logging, timeuuid
from komlogd.api.protocol import validation
from komlogd.api.protocol.processing import procedure as prproc
from komlogd.api.model.metrics import Datasource, Sample


class MetricStore:

    def __init__(self):
        self._dfs = {}
        self._synced_ranges = {}
        self._tr_dfs = {}
        self._tr_synced_ranges = {}
        self._hooked = set()
        self._metrics_info = {}

    async def sync(self):
        if getattr(self, '_prev_hooked', False):
            for metric in self._prev_hooked:
                resp = await self.hook(metric)
                if resp['hooked'] == False:
                    return False
            del self._prev_hooked
        return True

    def clear_synced(self):
        self._synced_ranges = {}
        self._tr_synced_ranges = {}
        for metric in self._hooked:
            if not getattr(self, '_prev_hooked',False):
                self._prev_hooked = set()
            self._prev_hooked.add(metric)
        self._hooked = set()

    def insert(self, metric, t, value):
        sample = Sample(metric=metric, t=t, value=value)
        tr = asyncio.Task.current_task().get_tr()
        if tr:
            tid = tr.tid
            self._store(sample.metric, sample.t, sample.value, tm=time.monotonic(), op='i', tid=tid)
            tr.add_dirty_item(self)
        else:
            self._store(sample.metric, sample.t, sample.value, tm=time.monotonic())

    async def get(self, metric, t=None, start=None, end=None, count=None):
        if t != None:
            its = t
            ets = t
        elif start == None and end == None:
            raise ValueError('You must set at least start or end')
        elif (start == None or end == None) and count == None:
            raise ValueError('count parameter must be set if you leave interval open')
        else:
            its = start
            ets = end
        total_regs = 0
        for r in self._get_missing_ranges(metric, its=its, ets=ets, count=count):
            resp = await self._request_data_range(metric, r['its'], r['ets'], count)
            if count:
                total_regs += resp['count']
                if count <= total_regs:
                    break
        return self._get_metric_data(metric, its, ets, count)

    async def _request_data_range(self, metric, its, ets, count):
        response = await prproc.request_data(metric, its, ets, count)
        tr = asyncio.Task.current_task().get_tr()
        if tr:
            tr.add_dirty_item(self)
            tid = tr.tid
            op = 'g'
        else:
            tid = None
            op = None
        d = response['data']
        for r in d:
            sample = Sample(metric, r[0], r[1])
            self._store(sample.metric, sample.t, sample.value, tm=time.monotonic(), op=op, tid=tid)
        if count != None and count > 0 and len(d) == count:
            its = min(r[0] for r in d)
            ets = max(r[0] for r in d)
        else:
            if its == None:
                if len(d) > 0 and count == None:
                    its = min(r[0] for r in d)
                else:
                    its = timeuuid.MIN_TIMEUUID
            if ets == None:
                if len(d) > 0 and count == None:
                    ets = max(r[0] for r in d)
                else:
                    ets = timeuuid.MAX_TIMEUUID
        if its and ets:
            self._add_synced_range(metric, time.monotonic(), its, ets, tid)
        return {'count':len(d)}

    def _store(self, metric, t, value, tm, op=None, tid=None):
        if isinstance(value, decimal.Decimal):
            tmp_value = int(value) if value%1 == 0 else float(value)
        else:
            tmp_value = value
        if tid:
            dfs = self._tr_dfs.get(tid, None)
            if dfs == None:
                dfs = {}
                self._tr_dfs[tid] = dfs
            df = dfs.get(metric, None)
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(columns=['t','value','op','value_orig'])
                dfs[metric] = df
            df.loc[tm]=[t, tmp_value, op, value]
        else:
            df = self._dfs.get(metric,None)
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(columns=['t','value'])
                self._dfs[metric] = df
            df.loc[tm]=[t, tmp_value]

    def _get_missing_ranges(self, metric, its, ets, count):
        def get_missing_open_interval(ranges):
            missing = []
            t = its if its != None else ets
            current_range = None
            for r in ranges:
                if ets and t <= r['ets'] and t > r['its']:
                    current_range = r
                    break
                elif its and t < r['ets'] and t >= r['its']:
                    current_range = r
                    break
            if current_range:
                r_border = current_range['its'] if ets else current_range['ets']
                t1, t2 = (t,r_border) if its else (r_border,t)
                elem = self._get_metric_data(metric, its=t1, ets=t2, count=count)
                if elem is not None and len(elem) < count:
                    new_count = count - len(elem)
                    if its == None and r_border > timeuuid.MIN_TIMEUUID:
                        missing_range = self._get_missing_ranges(metric, its=its, ets=r_border, count=new_count)
                        missing.extend(missing_range)
                    elif ets == None and r_border < timeuuid.MAX_TIMEUUID:
                        missing_range = self._get_missing_ranges(metric, its=r_border, ets=ets, count=new_count)
                        missing.extend(missing_range)
            else:
                r_border = None
                if its:
                    for r in ranges:
                        if r['its'] > its and r['its'] < timeuuid.MAX_TIMEUUID:
                            if r_border == None:
                                r_border = r['its']
                            elif r['its'] < r_border:
                                r_border = r['its']
                    if not r_border:
                        r_border = timeuuid.MAX_TIMEUUID
                    missing_range = {'its':its, 'ets':r_border}
                    missing.append(missing_range)
                    if r_border < timeuuid.MAX_TIMEUUID:
                        next_missing = self._get_missing_ranges(metric, its=r_border, ets=ets, count=count)
                        missing.extend(next_missing)
                elif ets:
                    for r in ranges:
                        if r['ets'] < ets and r['ets'] > timeuuid.MIN_TIMEUUID:
                            if r_border == None:
                                r_border = r['ets']
                            elif r['ets'] > r_border:
                                r_border = r['ets']
                    if not r_border:
                        r_border = timeuuid.MIN_TIMEUUID
                    missing_range = {'its':r_border, 'ets':ets}
                    missing.append(missing_range)
                    if r_border > timeuuid.MIN_TIMEUUID:
                        next_missing = self._get_missing_ranges(metric, its=its, ets=r_border, count=count)
                        missing.extend(next_missing)
            return missing
        def get_missing(ranges):
            missing = [{'its':its, 'ets':ets}]
            while True:
                loop_missing = []
                for miss in missing:
                    keep = True
                    for r in ranges:
                        if miss['its'] < r['its']:
                            if miss['ets'] > r['ets']:
                                # slice it
                                loop_missing.append({'its':miss['its'], 'ets':r['its']})
                                loop_missing.append({'its':r['ets'], 'ets':miss['ets']})
                                keep = False
                                break
                            elif miss['ets'] > r['its']:
                                # keep lower
                                loop_missing.append({'its':miss['its'], 'ets':r['its']})
                                keep = False
                                break
                        elif miss['ets'] > r['ets']:
                            if miss['its'] < r['its']:
                                # slice it
                                loop_missing.append({'its':miss['its'], 'ets':r['its']})
                                loop_missing.append({'its':r['ets'], 'ets':miss['ets']})
                                keep = False
                                break
                            elif miss['its'] < r['ets']:
                                # keep higher
                                loop_missing.append({'its':r['ets'], 'ets':miss['ets']})
                                keep = False
                                break
                        elif r['its'] <= miss['its'] and r['ets'] >= miss['ets']:
                            # remove it
                            keep = False
                            break
                    if keep:
                        # did not match any synced, keep it
                        loop_missing.append(miss)
                loop_missing = sorted(loop_missing, key=lambda x:x['ets'])
                if missing == loop_missing:
                    return missing
                missing = loop_missing
        tr = asyncio.Task.current_task().get_tr()
        if tr:
            ranges = self._tr_synced_ranges.get(tr.tid,None)
            if ranges == None:
                ranges = {}
                self._tr_synced_ranges[tr.tid]=ranges
            m_ranges = ranges.get(metric, None)
            if m_ranges == None:
                m_ranges = [r for r in self._synced_ranges.get(metric, []) if r['t']<=tr.tm]
                ranges[metric] = m_ranges
        else:
            m_ranges = self._synced_ranges.get(metric, None)
            if m_ranges == None:
                m_ranges = []
                self._synced_ranges[metric] = m_ranges
        if (its == None or ets == None) and count != None:
            missing = get_missing_open_interval(m_ranges)
        else:
            missing = get_missing(m_ranges)
        return missing

    def _add_synced_range(self, metric, t, its, ets, tid=None):
        def add_new_range(new, existing):
            final = [new]
            overlaps = []
            for r in existing:
                if r['ets'] <= ets and r['its'] >= its:
                    continue
                elif r['ets'] > its and r['its'] <= its:
                    overlaps.append(r)
                elif r['its'] < ets and r['ets'] >= ets:
                    overlaps.append(r)
                elif r['its'] <= its and r['ets'] >= ets:
                    overlaps.append(r)
                else:
                    final.append(r)
            for r in overlaps:
                if r['its'] < its and r['ets'] > ets:
                    final.append({'t':r['t'], 'its':r['its'], 'ets':its})
                    final.append({'t':r['t'], 'its':ets, 'ets':r['ets']})
                elif r['its'] < its and r['ets'] >= its:
                    final.append({'t':r['t'], 'its':r['its'], 'ets':its})
                elif r['its'] <= ets and r['ets'] > ets:
                    final.append({'t':r['t'], 'its':ets, 'ets':r['ets']})
                elif r['its'] >= its and r['ets'] <= ets:
                    continue
            final.sort(key = lambda r: r['its'])
            return final
        if tid:
            ranges = self._tr_synced_ranges.get(tid, None)
            if ranges == None:
                ranges = {}
                self._tr_synced_ranges[tid]=ranges
            m_ranges = ranges.get(metric, None)
            if m_ranges == None:
                m_ranges = []
                m_ranges.append({'t':t,'its':its,'ets':ets})
                ranges[metric] = m_ranges
            else:
                new_range = {'t':t,'its':its,'ets':ets}
                new_ranges = add_new_range(new_range, m_ranges)
                ranges[metric] = new_ranges
        elif metric in self._hooked:
            ranges = self._synced_ranges.get(metric, [])
            new_range = {'t':t, 'its':its, 'ets':ets}
            new_ranges = add_new_range(new_range, ranges)
            self._synced_ranges[metric] = new_ranges

    def _get_metric_data(self, metric, its, ets, count):
        if its == None:
            its = timeuuid.MIN_TIMEUUID
        if ets == None:
            ets = timeuuid.MAX_TIMEUUID
        if count and ets == None:
            ascending = False
        else:
            ascending = True
        tr = asyncio.Task.current_task().get_tr()
        if tr:
            # get transaction dataframe if exists
            tid = tr.tid
            tr_dfs = self._tr_dfs.get(tid, None)
            if tr_dfs == None:
                tr_df = None
            else:
                tr_df = tr_dfs.get(metric,None)
        else:
            tr_df = None
        df = self._dfs.get(metric, None)
        have_df, have_tr_df = isinstance(df, pd.DataFrame), isinstance(tr_df, pd.DataFrame)
        if have_df and have_tr_df:
            df = df[(df.index<=tr.tm) & (df.t.between(its,ets))][['t','value']]
            tr_df = tr_df[tr_df.t.between(its,ets)][['t','value']]
            # in concat, tr_df at the end, because its index will always be higher. No need to sort it.
            join_df = pd.concat([df,tr_df])
        elif have_df:
            join_df = df[df.t.between(its,ets)][['t','value']]
        elif have_tr_df:
            join_df = tr_df[tr_df.t.between(its,ets)][['t','value']]
        else:
            return None
        if join_df.empty:
            return None
        else:
            s = pd.Series(index=join_df.t, data=join_df.value.values)
            s = s[~s.index.duplicated(keep='last')].sort_index(ascending=ascending)
            s.name = metric
            if count != None:
                return s.iloc[-count:]
            return s

    async def hook(self, metric):
        result = await prproc.hook_to_metric(metric)
        if result['hooked']:
            self._hooked.add(metric)
            if result['exists']:
                #sync future
                now = timeuuid.TimeUUID()
                await self.get(metric, start=now, end=timeuuid.MAX_TIMEUUID, count=200)
            else:
                self._add_synced_range(metric, t=time.monotonic(), its=timeuuid.MIN_TIMEUUID, ets=timeuuid.MAX_TIMEUUID)
        return result

    def is_in(self, metric, t, value):
        ''' Returns False if tuple (metric,t,value) is not found. Only checks the last value '''
        if not metric in self._dfs:
            return False
        df = self._dfs[metric]
        if not t in df.t.values:
            return False
        else:
            if isinstance(value, decimal.Decimal):
                tmp_value = int(value) if value%1 == 0 else float(value)
            else:
                tmp_value = value
            smpls = df[df.t == t]
            if not smpls.value.values[-1] == tmp_value:
                return False
        return True

    def has_updates(self, metric, t, tm):
        ''' Returns True if tuple (metric,t) has newer rows than tm '''
        if not metric in self._dfs:
            return False
        df = self._dfs[metric]
        if not t in df.t.values:
            return False
        smpls = df[df.t == t]
        if not smpls[smpls.index > tm].empty:
            return True
        return False

    async def _tr_commit(self, tr):
        i_samples = []
        g_samples = []
        for metric, df in self._tr_dfs.get(tr.tid, {}).items():
            i_smpls = df[df.op == 'i']
            i_smpls = i_smpls[~i_smpls.t.duplicated(keep='last')]
            for index, row in i_smpls.iterrows():
                i_samples.append(Sample(metric=metric, t=row.t, value=row.value_orig))
            g_smpls = df[df['op'] == 'g']
            g_smpls = g_smpls[~g_smpls.t.duplicated(keep='last')]
            if metric in self._hooked:
                for index, row in g_smpls.iterrows():
                    g_samples.append((index,Sample(metric=metric, t=row.t, value=row.value)))
        for t,sample in g_samples:
            self._store(metric=sample.metric, t=sample.t, value=sample.value,tm=index)
        for metric, ranges in self._tr_synced_ranges.get(tr.tid, {}).items():
            if metric in self._hooked:
                for r in ranges:
                    self._add_synced_range(metric, r['t'], r['its'], r['ets'])
        if len(i_samples) > 0:
            await prproc.send_samples(i_samples, irt=tr.irt)
            items = [s.metric for s in i_samples if isinstance(s.metric, Datasource) and s.metric.supplies != None]
            info_metrics = set()
            for m in items:
                if not m in self._metrics_info or sorted(m.supplies) != self._metrics_info[m]['supplies']:
                    info_metrics.add(m)
            info_metrics = list(info_metrics)
            if len(info_metrics)>0:
                await prproc.send_info(list(info_metrics), irt=tr.irt)
                for m in info_metrics:
                    try:
                        self._metrics_info[m]['supplies'] = sorted(m.supplies)
                    except KeyError:
                        self._metrics_info[m] = {'supplies':sorted(m.supplies)}

    def _tr_discard(self, tr):
        self._tr_dfs.pop(tr.tid, None)
        self._tr_synced_ranges.pop(tr.tid, None)

