'''

scheduler methods

'''

import os
import yaml
import traceback
from komlogd.base import config, logging
from komlogd.base.settings import defaults, templates, options
from komlogd.scheduler.model import scheduler, exceptions

Scheduler = None

def initialize_scheduler():
    global Scheduler
    entries = config.config.get_entries(entryname=options.ENTRY_JOB)
    jobs=[]
    for entry in entries:
        try:
            uri=entry[options.JOB_URI]
            command=entry[options.JOB_COMMAND]
            enabled=entry[options.JOB_ENABLED]
            schedule=entry[options.JOB_SCHEDULE]
            job = scheduler.ScheduledJob(uri=uri,command=command,enabled=enabled,schedule=schedule)
            logging.logger.debug('adding job: '+str(job.__dict__))
            jobs.append(job)
        except Exception:
            logging.logger.error('Error loading job configuration.')
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
    if len(jobs) == 0:
        logging.logger.info('No jobs configured.')
    Scheduler = scheduler.Scheduler(jobs=jobs)

def start_scheduler():
    global Scheduler
    Scheduler.start()

