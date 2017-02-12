import os
from komlogd.api import crypto
from komlogd.base import config, exceptions, logging
from komlogd.base.settings import defaults, options

def get_private_key():
    keys=config.config.get_entries(entryname=options.KOMLOG_KEYFILE)
    if len(keys)==0:
        privkey_file=os.path.join(config.config.root_dir,defaults.RSA_PRIV_KEY)
    elif len(keys)>1:
        logging.logger.error('More than one keyfile entries found in configuration. Keep only one.')
        return None
    else:
        privkey_file=keys[0]
    if not os.path.isfile(privkey_file):
        logging.logger.debug('Generating RSA keys...')
        key_dir=os.path.split(privkey_file)[0]
        pubkey_file=os.path.join(key_dir,defaults.RSA_PUB_KEY)
        privkey=crypto.generate_rsa_key()
        if privkey and crypto.store_keys(privkey=privkey, privkey_file=privkey_file, pubkey_file=pubkey_file):
            logging.logger.debug('Keys stored successfully on disk')
            pubkey=privkey.public_key()
            key_str=crypto.get_printable_pubkey(pubkey)
            logging.logger.info('This is the public key, add it to your komlog account:\n'+key_str)
            return privkey
    else:
        privkey=crypto.load_private_key(privkey_file)
        return privkey

def get_public_key():
    privkey=get_private_key()
    if not privkey:
        logging.logger.error('Error obtaining public key.')
        return None
    else:
        pubkey=privkey.public_key()
        key_str=crypto.get_printable_pubkey(pubkey)
        logging.logger.info('Public key is:\n'+key_str)
        return pubkey

