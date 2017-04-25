'''
Templates file

'''

TEMPLATE_CONFIG_FILE='''
# komlogd configuration file

# username to connect to komlog. Set it and uncomment the following line:
#- username: username

# if you want to specify a different private key file, uncomment this line and set the filename:
#- keyfile: /home/komlogd/.komlogd/key.priv

# logging configuration, default values are indicated:
#- logging:
#    level: INFO
#    rotation: yes
#    max_bytes: 10000000
#    backup_count: 3
#    dirname: log
#    filename: komlogd.log


# Create an entry like the next one for every transfers file you want to load:
#- transfers:
#   enabled: yes
#   filename: /home/komlog/.komlogd/transfers/transfer_methods.py

# In the previous example, you can set the filename with the relative path from the application directory, too.
# Application directory is the one where the configuration file is located.
# So, if application directory is /home/komlog/.komlogd, we can set the previous filename as:
# filename: transfers/transfer_methods.py

'''

