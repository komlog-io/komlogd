import os
from base64 import b64encode, b64decode
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from komlogd.api.model import exceptions

def load_private_key(privkey_file):
    ''' 
    Loads the private key stored in the file indicated by the privkey_file
    parameter and returns it

    The `privkey_file` parameter indicates the absolute path to the file
    storing the private key.

    The key returned is a RSAPrivateKey instance.

    If there is a problem accessing the file, the exception will be raised
    without modification.

    If the problem occurs loading the key, a CryptoException will be raised.
    '''
    with open(privkey_file, "rb") as key_file:
        try:
            privkey = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
            if isinstance(privkey, rsa.RSAPrivateKey):
                return privkey
        except Exception as e:
            raise exceptions.CryptoException()

def load_public_key(pubkey_file):
    ''' 
    Loads the public key stored in the file indicated by the pubkey_file
    parameter and returns it

    The `pubkey_file` parameter indicates the absolute path to the file
    storing the public key.

    The key returned is a RSAPublicKey instance.

    If there is a problem accessing the file, the exception will be raised
    without modification.

    If the problem occurs loading the key, a CryptoException will be raised.
    '''
    with open(pubkey_file, "rb") as key_file:
        try:
            pubkey = serialization.load_pem_public_key(
                key_file.read(),
                backend=default_backend()
            )
            if isinstance(pubkey, rsa.RSAPublicKey):
                return pubkey
        except Exception as e:
            raise exceptions.CryptoException()

def serialize_public_key(key):
    '''
    Returns the public key serialization in base64
    '''
    try:
        pem = key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return b64encode(pem).decode()
    except Exception:
        return None

def serialize_private_key(key):
    '''
    Returns the private key serialization in base64
    '''
    try:
        pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return b64encode(pem).decode()
    except Exception:
        return None

def decrypt(privkey, ciphertext):
    try:
        plaintext = privkey.decrypt(
            ciphertext,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA1()),
                algorithm=hashes.SHA1(),
                label=None
            )
        )
        return plaintext
    except Exception:
        return None

def encrypt(pubkey, plaintext):
    try:
        ciphertext= pubkey.encrypt(plaintext=plaintext,
            padding=padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA1()),
                algorithm=hashes.SHA1(),
                label=None
            )
        )
        return ciphertext
    except Exception:
        return None

def sign_message(privkey, message):
    try:
        plaintext = b64decode(message.encode('utf-8'))
        signer = privkey.signer(
            padding.PSS(
                mgf=padding.MGF1(hashes.Whirlpool()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.Whirlpool()
        )
        signer.update(plaintext)
        return b64encode(signer.finalize()).decode('utf-8')
    except Exception:
        return None

def get_hash(message):
    try:
        digest = hashes.Hash(hashes.Whirlpool(), backend=default_backend())
        for i in range(0,30):
            digest.update(message)
        return digest.finalize()
    except Exception:
        return None

def process_challenge(privkey, challenge):
    try:
        ciphertext = b64decode(challenge.encode('utf-8'))
        plaintext = decrypt(privkey=privkey, ciphertext=ciphertext)
        hashed = get_hash(plaintext)
        return b64encode(hashed).decode('utf-8')
    except Exception:
        return None

def generate_rsa_key(key_size=4096):
    privkey = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
        backend=default_backend()
    )
    return privkey

def store_keys(privkey, privkey_file, pubkey_file):
    if not isinstance(privkey,rsa.RSAPrivateKey):
        return False
    privkey_serial = privkey.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )
    pubkey = privkey.public_key()
    pubkey_serial = pubkey.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    priv_stored=False
    try:
        with open(privkey_file,'wb') as privkey_out:
            privkey_out.write(privkey_serial)
            privkey_out.close()
            priv_stored=True
        with open(pubkey_file,'wb') as pubkey_out:
            pubkey_out.write(pubkey_serial)
            pubkey_out.close()
    except Exception:
        if priv_stored:
            os.remove(privkey_file)
        return False
    return True

def get_printable_pubkey(pubkey):
    if not isinstance(pubkey, rsa.RSAPublicKey):
        return None
    pem = pubkey.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return pem.decode()

