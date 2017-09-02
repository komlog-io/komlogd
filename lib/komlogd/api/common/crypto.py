import os
import traceback
from base64 import b64encode, b64decode
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from komlogd.api.common import exceptions, logging

def load_private_key(privkey_file):
    ''' 
    Loads the private key stored in the file indicated by the privkey_file
    parameter and returns it

    The `privkey_file` parameter indicates the absolute path to the file
    storing the private key.

    The key returned is a RSAPrivateKey instance.
    '''
    with open(privkey_file, "rb") as key_file:
        privkey = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return privkey

def load_public_key(pubkey_file):
    ''' 
    Loads the public key stored in the file indicated by the pubkey_file
    parameter and returns it

    The `pubkey_file` parameter indicates the absolute path to the file
    storing the public key.

    The key returned is a RSAPublicKey instance.
    '''
    with open(pubkey_file, "rb") as key_file:
        pubkey = serialization.load_ssh_public_key(
            key_file.read(),
            backend=default_backend()
        )
    return pubkey

def serialize_public_key(key):
    '''
    Returns the public key serialization in base64
    '''
    pem = key.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH
    )
    return b64encode(pem).decode()

def serialize_private_key(key):
    '''
    Returns the private key serialization in base64
    '''
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return b64encode(pem).decode()

def decrypt(privkey, ciphertext):
    plaintext = privkey.decrypt(
        ciphertext,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return plaintext

def encrypt(pubkey, plaintext):
    ciphertext= pubkey.encrypt(plaintext=plaintext,
        padding=padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return ciphertext

def sign_message(privkey, message):
    plaintext = b64decode(message.encode('utf-8'))
    signature = privkey.sign(
        plaintext,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return b64encode(signature).decode('utf-8')

def get_hash(message):
    digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
    for i in range(0,30):
        digest.update(message)
    return digest.finalize()

def process_challenge(privkey, challenge):
    ciphertext = b64decode(challenge.encode('utf-8'))
    plaintext = decrypt(privkey=privkey, ciphertext=ciphertext)
    hashed = get_hash(plaintext)
    return b64encode(hashed).decode('utf-8')

def generate_rsa_key(key_size=4096):
    privkey = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
        backend=default_backend()
    )
    return privkey

def store_keys(privkey, privkey_file, pubkey_file):
    privkey_serial = privkey.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    pubkey = privkey.public_key()
    pubkey_serial = pubkey.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH
    )
    with os.fdopen(os.open(privkey_file, os.O_WRONLY | os.O_CREAT, 0o600), 'wb') as privkey_out:
        privkey_out.write(privkey_serial)
    try:
        with os.fdopen(os.open(pubkey_file, os.O_WRONLY | os.O_CREAT, 0o644), 'wb') as pubkey_out:
            pubkey_out.write(pubkey_serial)
    except:
        os.remove(privkey_file)
        raise

def get_printable_pubkey(pubkey):
    pem = pubkey.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH
    )
    return pem.decode()

