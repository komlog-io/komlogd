import os
from komlogd.api.common import crypto
from komlogd.base import config, exceptions, logging
from komlogd.base.settings import defaults

def get_private_key():
    privkey_file = config.config.key
    if not os.path.isfile(privkey_file):
        logging.logger.debug('Generating RSA keys...')
        key_dir=os.path.dirname(privkey_file)
        pubkey_file=os.path.join(key_dir,defaults.RSA_PUB_KEY)
        privkey=crypto.generate_rsa_key()
        crypto.store_keys(privkey=privkey, privkey_file=privkey_file, pubkey_file=pubkey_file)
        logging.logger.debug('Keys stored successfully on disk')
        pubkey=privkey.public_key()
        key_str=crypto.get_printable_pubkey(pubkey)
        logging.logger.info('This is the public key, add it to your Komlog account:\n'+key_str)
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

