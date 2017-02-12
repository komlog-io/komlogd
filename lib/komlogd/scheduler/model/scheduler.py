'''

scheduler

'''

import asyncio
import time
import traceback
from datetime import datetime
from komlogd.base import logging
from komlogd.api.model import orm
from komlogd.web import main as webmain

class Scheduler:
    def __init__(self, jobs):
        self.loop = asyncio.get_event_loop()
        self.jobs = jobs

    def start(self):
        now = time.time()
        when = self.loop.time()+(60-now%60)
        localtime = time.localtime(now+(60-now%60))
        for job in self.jobs:
            if job.enabled:
                logging.logger.debug('Programming job loop: '+job.uri)
                self.loop.call_at(when, self.job_loop, job, localtime)

    def job_loop(self, job, localtime):
        logging.logger.debug('Entering job loop: '+job.uri)
        if job.matchs(localtime):
            logging.logger.debug('Enqueueing job exec: '+job.uri)
            self.loop.create_task(self.job_exec(job, localtime))
        else:
            now = time.time()
            when = self.loop.time()+(60-now%60)
            localtime = time.localtime(now+(60-now%60))
            logging.logger.debug('Programming job loop: '+job.uri)
            self.loop.call_at(when, self.job_loop, job, localtime)

    async def job_exec(self, job, localtime):
        logging.logger.debug('Launching job execution: '+job.uri)
        logging.logger.debug('command to execute: '+str(job.command))
        try:
            create = asyncio.create_subprocess_exec(*job.command, stdout=asyncio.subprocess.PIPE, stderr = asyncio.subprocess.PIPE)
            process = await create
            output = await process.stdout.read()
        except Exception:
            logging.logger.error('Exception running job.')
            ex_info = traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
            now = time.time()
            when = self.loop.time()+(60-now%60)
            localtime = time.localtime(now+(60-now%60))
            logging.logger.debug('Rescheduling job check: '+job.uri)
            self.loop.call_at(when, self.job_loop, job, localtime)
        else:
            logging.logger.debug('Job output '+job.uri)
            logging.logger.debug(output.decode('utf-8'))
            await process.wait()
            now = time.time()
            data=output.decode('utf-8')
            ts=datetime.strptime(time.strftime('%Y-%m-%dT%H:%M:%S%z',localtime),'%Y-%m-%dT%H:%M:%S%z')
            when = self.loop.time()+(60-now%60)
            localtime = time.localtime(now+(60-now%60))
            logging.logger.debug('Rescheduling job check: '+job.uri)
            self.loop.call_at(when, self.job_loop, job, localtime)
            if len(data)>0:
                metric = orm.Datasource(uri=job.uri)
                sample = orm.Sample(metric=metric, ts=ts, data=data)
                logging.logger.debug('Sending content to Komlog: '+metric.uri)
                webmain.send_sample(sample)

class ScheduledJob:
    def __init__(self, uri, command, enabled, schedule):
        self.uri = uri 
        self.command=command.split(' ')
        self.enabled = enabled
        self.schedule=self._process_schedule(schedule)
        if len(self.command)==0:
            raise exceptions.SchedulerException
            
    def _process_schedule(self, schedule):
        def process_var(var,max_value,min_value):
            processed_entry=[]
            in_range=range(min_value,max_value+1)
            try:
                int_v=int(var)
                processed_entry.append(int_v) if (int_v<=max_value and int_v>=min_value) else None
            except Exception:
                if var=='*':
                    for i in range(min_value,max_value+1):
                        processed_entry.append(i)
                elif len(var.split(','))>1:
                    for group in var.split(','):
                        result=process_var(group,max_value,min_value)
                        for value in result:
                            processed_entry.append(value)
                elif len(var.split('-'))>1:
                    r=var.split('-')
                    r_min=int(r[0])
                    r_max=int(r[1])
                    for i in range(r_min,r_max+1):
                        if i in in_range:
                            processed_entry.append(i) 
                elif len(var.split('/'))>1:
                    num,den=var.split('/')
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
        processed_schedule=[]
        if isinstance(schedule,str):
            schedule=[schedule]
        for entry in schedule:
            entry_sched={}
            try:
                minute,hour,dow,month,dom=entry.split(' ')
            except Exception as e:
                logging.logger.exception('Exception processing schedule entry')
                logging.logger.exception(str(e))
            else:
                entry_sched['minute']=process_var(minute,59,0)
                entry_sched['hour']=process_var(hour,23,0)
                entry_sched['dow']=process_var(dow,6,0)
                entry_sched['month']=process_var(month,12,1)
                entry_sched['dom']=process_var(dom,31,1)
                processed_schedule.append(entry_sched)
        return processed_schedule

    def get_next_execution_date(self,init_date):
        '''implementing this is pending. should return the date of the next execution, so we can program the execution at that time '''
        return None

    def matchs(self, t):
        def check_entry(entry,t):
            if (t.tm_min in entry['minute'] and
                t.tm_hour in entry['hour'] and
                t.tm_wday in entry['dow'] and
                t.tm_mon in entry['month'] and
                t.tm_mday in entry['dom']):
                logging.logger.debug(str(entry)+' matchs '+str(t))
                return True
            logging.logger.debug(str(entry)+' DOES NOT match '+str(t))
            return False
        for entry in self.schedule:
            if check_entry(entry,t):
                return True
        return False

