'''
Templates file

'''

TEMPLATE_CONFIG_FILE='''


# komlogd configuration file




# username to connect to komlog. Set it and uncomment the following line
#- username: username

# if you want to specify the private key, uncomment this line and set the file path
#- keyfile: /home/komlogd/.komlogd/key.priv

# logging configuration, default values are indicated
#- logging:
#    level: INFO
#    rotation: yes
#    max_bytes: 10000000
#    backup_count: 3
#    dirname: log
#    filename: komlogd.log


# You can load jobs configuration from other files, to enable this option uncomment the following
# key and set it to yes
#- allow_external_jobs: no


# if you enabled the option above, add an entry for each file you want to load jobs from.
#- external_job_file: /path/to/ext/job/file


# Create an entry for each scheduled job. The following blocks define two jobs:

#- job:
#    uri: system.disk.ocupation
#    enabled: yes
#    command: df -k    # it can be a command, a script, etc
#    schedule:         # crontab style schedule
#        - '0 * * * *'          # don't forget the quotes
#        - '*/5 23 * * *'       # support for multiple schedules.

#- job:
#    uri: system.memory
#    enabled: yes
#    command: /home/komlogd/my_script.sh
#    schedule:
#        - '* * * * *'


# Create an entry for each python file with impulses you want to load. The following is an example:

#- impulses:
#   enabled: yes
#   filename: /home/komlog/.komlogd/impulses/impulse_methods.py 

# In the previous example, we can indicate the filename with a relative path too from the application directory.
# Application directory is the one where the configuration file is located.
#So, for example, if application directory is /home/komlog/.komlogd, we can indicate the previous file with:
# filename: impulses/impulse_methods.py

'''

