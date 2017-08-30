import unittest
import os
import random
import string
from base64 import b64encode, b64decode
from komlogd.api.common import crypto, exceptions
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.exceptions import UnsupportedAlgorithm

class ApiCommonCryptoTest(unittest.TestCase):

    def test_load_private_key_failure_non_existent_file(self):
        ''' load_private_key should fail if filename does not exists '''
        filename='/a/path/to/nonexistentfile'
        with self.assertRaises(OSError) as cm:
            crypto.load_private_key(filename)
        self.assertEqual(cm.exception.errno, 2)

    def test_load_private_key_failure_filename_passed_is_not_a_file(self):
        ''' load_private_key should fail if filename passed is not a file '''
        filename='/tmp'
        self.assertTrue(os.path.isdir(filename))
        with self.assertRaises(OSError) as cm:
            crypto.load_private_key(filename)
        self.assertEqual(cm.exception.errno, 21)

    def test_load_private_key_failure_no_read_permission(self):
        ''' load_private_key should fail if we have no read permission over filename '''
        filename='/tmp/random_file_'+''.join(random.SystemRandom().choice(string.ascii_uppercase
            + string.digits) for _ in range(10))
        with os.fdopen(os.open(filename, os.O_WRONLY | os.O_CREAT, 0o200), 'w') as handle:
          handle.write('')
        with self.assertRaises(OSError) as cm:
            crypto.load_private_key(filename)
        os.remove(filename)
        self.assertEqual(cm.exception.errno, 13)

    def test_load_private_key_failure_invalid_key_file(self):
        ''' load_private_key should fail if we pass an invalid key file '''
        filename='/tmp/random_file_'+''.join(random.SystemRandom().choice(string.ascii_uppercase
            + string.digits) for _ in range(10))
        with os.fdopen(os.open(filename, os.O_WRONLY | os.O_CREAT, 0o600), 'w') as handle:
          handle.write('invalid_key')
        with self.assertRaises(ValueError) as cm:
            crypto.load_private_key(filename)
        os.remove(filename)

    def test_load_private_key_failure_public_key_filename_passed(self):
        ''' load_private_key should fail if we pass the public key file instead of the private one '''
        random_string=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(10))
        privkey_file=os.path.join('/tmp/',random_string+'.priv')
        pubkey_file=os.path.join('/tmp/',random_string+'.pub')
        privkey=crypto.generate_rsa_key()
        pubkey_generated=privkey.public_key()
        crypto.store_keys(privkey=privkey,privkey_file=privkey_file,pubkey_file=pubkey_file)
        with self.assertRaises(ValueError) as cm:
            loadedkey=crypto.load_private_key(pubkey_file)
        os.remove(privkey_file)
        os.remove(pubkey_file)

    def test_load_private_key_success(self):
        ''' load_private_key should succeed and return the private key '''
        random_string=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(10))
        privkey_file=os.path.join('/tmp/',random_string+'.priv')
        pubkey_file=os.path.join('/tmp/',random_string+'.pub')
        privkey=crypto.generate_rsa_key()
        pubkey_generated=privkey.public_key()
        crypto.store_keys(privkey=privkey,privkey_file=privkey_file,pubkey_file=pubkey_file)
        loadedkey=crypto.load_private_key(privkey_file)
        pubkey_loaded=privkey.public_key()
        os.remove(privkey_file)
        os.remove(pubkey_file)
        self.assertIsNotNone(loadedkey)
        self.assertEqual(crypto.serialize_public_key(pubkey_generated),crypto.serialize_public_key(pubkey_loaded))

    def test_load_public_key_failure_non_existent_file(self):
        ''' load_public_key should fail if filename does not exists '''
        filename='/a/path/to/nonexistentfile'
        with self.assertRaises(OSError) as cm:
            crypto.load_public_key(filename)
        self.assertEqual(cm.exception.errno, 2)

    def test_load_public_key_failure_filename_passed_is_not_a_file(self):
        ''' load_public_key should fail if filename passed is not a file '''
        filename='/tmp'
        self.assertTrue(os.path.isdir(filename))
        with self.assertRaises(OSError) as cm:
            crypto.load_public_key(filename)
        self.assertEqual(cm.exception.errno, 21)

    def test_load_public_key_failure_no_read_permission(self):
        ''' load_public_key should fail if we have no read permission over filename '''
        filename='/tmp/random_file_'+''.join(random.SystemRandom().choice(string.ascii_uppercase
            + string.digits) for _ in range(10))
        with os.fdopen(os.open(filename, os.O_WRONLY | os.O_CREAT, 0o200), 'w') as handle:
          handle.write('')
        with self.assertRaises(OSError) as cm:
            crypto.load_public_key(filename)
        os.remove(filename)
        self.assertEqual(cm.exception.errno, 13)

    def test_load_public_key_failure_invalid_key_file(self):
        ''' load_public_key should fail if we pass an invalid key file '''
        filename='/tmp/random_file_'+''.join(random.SystemRandom().choice(string.ascii_uppercase
            + string.digits) for _ in range(10))
        with os.fdopen(os.open(filename, os.O_WRONLY | os.O_CREAT, 0o600), 'w') as handle:
          handle.write('invalid_key')
        with self.assertRaises(ValueError) as cm:
            crypto.load_public_key(filename)
        os.remove(filename)

    def test_load_public_key_failure_private_key_filename_passed(self):
        ''' load_public_key should fail if we pass the private key file instead of the private one '''
        random_string=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(10))
        privkey_file=os.path.join('/tmp/',random_string+'.priv')
        pubkey_file=os.path.join('/tmp/',random_string+'.pub')
        privkey=crypto.generate_rsa_key()
        pubkey_generated=privkey.public_key()
        crypto.store_keys(privkey=privkey,privkey_file=privkey_file,pubkey_file=pubkey_file)
        with self.assertRaises(UnsupportedAlgorithm) as cm:
            loadedkey=crypto.load_public_key(privkey_file)
        os.remove(privkey_file)
        os.remove(pubkey_file)

    def test_load_public_key_success(self):
        ''' load_public_key should succeed and return the public key '''
        random_string=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(10))
        privkey_file=os.path.join('/tmp/',random_string+'.priv')
        pubkey_file=os.path.join('/tmp/',random_string+'.pub')
        privkey=crypto.generate_rsa_key()
        pubkey_generated=privkey.public_key()
        crypto.store_keys(privkey=privkey,privkey_file=privkey_file,pubkey_file=pubkey_file)
        pubkey_loaded=crypto.load_public_key(pubkey_file)
        os.remove(privkey_file)
        os.remove(pubkey_file)
        self.assertIsNotNone(pubkey_loaded)
        self.assertEqual(crypto.serialize_public_key(pubkey_generated),crypto.serialize_public_key(pubkey_loaded))

    def test_serialize_public_key_failure_invalid_key(self):
        ''' serialize_public_key should fail if key parameter is not a valid public key '''
        key='invalid_key'
        with self.assertRaises(AttributeError) as cm:
            crypto.serialize_public_key(key)

    def test_serialize_public_key_failure_private_key_passed(self):
        ''' serialize_public_key should fail if key parameter is not the public key but the private one '''
        privkey=crypto.generate_rsa_key()
        with self.assertRaises(AttributeError) as cm:
            crypto.serialize_public_key(privkey)

    def test_serialize_public_key_success(self):
        ''' serialize_public_key should succeed if we pass a valid public key '''
        privkey=crypto.generate_rsa_key()
        pubkey=privkey.public_key()
        self.assertIsNotNone(crypto.serialize_public_key(pubkey))

    def test_serialize_private_key_failure_invalid_key(self):
        ''' serialize_private_key should fail if key parameter is not a valid public key '''
        key='invalid_key'
        with self.assertRaises(AttributeError) as cm:
            crypto.serialize_private_key(key)

    def test_serialize_private_key_failure_public_key_passed(self):
        ''' serialize_private_key should fail if key parameter is not the private key but the public one '''
        privkey=crypto.generate_rsa_key()
        pubkey=privkey.public_key()
        with self.assertRaises(AttributeError) as cm:
            crypto.serialize_private_key(pubkey)

    def test_serialize_private_key_success(self):
        ''' serialize_private_key should succeed if we pass a valid private key '''
        privkey=crypto.generate_rsa_key()
        self.assertIsNotNone(crypto.serialize_private_key(privkey))

    def test_decrypt_failure_invalid_private_key(self):
        ''' decrypt should fail if private key is invalid '''
        privkey='privkey'
        ciphertext='ciphertext'.encode()
        with self.assertRaises(AttributeError) as cm:
            crypto.decrypt(privkey=privkey, ciphertext=ciphertext)

    def test_decrypt_failure_invalid_ciphertext(self):
        ''' decrypt should fail if ciphertext is not of type bytes '''
        privkey=crypto.generate_rsa_key()
        ciphertext='string type'
        with self.assertRaises(ValueError) as cm:
            crypto.decrypt(privkey=privkey, ciphertext=ciphertext)

    def test_decrypt_success(self):
        ''' decrypt should succeed '''
        privkey=crypto.generate_rsa_key()
        pubkey=privkey.public_key()
        text='text to encrypt'.encode()
        ciphertext=crypto.encrypt(pubkey,text)
        self.assertEqual(crypto.decrypt(privkey=privkey, ciphertext=ciphertext), text)

    def test_encrypt_failure_invalid_pubkey(self):
        ''' encrypt should fail if pubkey is not valid '''
        pubkey='pubkey'
        plaintext='text to encrypt'.encode()
        with self.assertRaises(AttributeError) as cm:
            crypto.encrypt(pubkey,plaintext)

    def test_encrypt_failure_invalid_plaintext(self):
        ''' encrypt should fail if pubkey is not valid bytes encoded text '''
        privkey=crypto.generate_rsa_key()
        pubkey=privkey.public_key()
        plaintext='text to encrypt is a string'
        with self.assertRaises(TypeError) as cm:
            crypto.encrypt(pubkey,plaintext)

    def test_encrypt_success(self):
        ''' encrypt should succeed '''
        privkey=crypto.generate_rsa_key()
        pubkey=privkey.public_key()
        plaintext='text to encrypt is bytes encoded'.encode()
        self.assertIsNotNone(crypto.encrypt(pubkey,plaintext))

    def test_sign_message_failure_invalid_privkey(self):
        ''' sign_message should return None if privkey is invalid '''
        privkey='privkey'
        message=b64encode('message'.encode()).decode()
        with self.assertRaises(AttributeError) as cm:
            crypto.sign_message(privkey,message)

    def test_sign_message_failure_invalid_message(self):
        ''' sign_message should return None if message is not a string '''
        privkey=crypto.generate_rsa_key()
        message=234234234234
        with self.assertRaises(AttributeError) as cm:
            crypto.sign_message(privkey,message)

    def test_sign_message_success(self):
        ''' sign_message should return the message signed '''
        privkey=crypto.generate_rsa_key()
        message=b64encode('message'.encode()).decode()
        signed=crypto.sign_message(privkey, message)
        self.assertIsNotNone(signed)
        self.assertTrue(isinstance(signed, str))

    def test_get_hash_failure_invalid_message(self):
        ''' get_hash should return None if message is not a bytes variable '''
        message='message'
        with self.assertRaises(TypeError) as cm:
            crypto.get_hash(message)

    def test_get_hash_success(self):
        ''' get_hash should return the hash of the message passed '''
        message='message'.encode()
        self.assertIsNotNone(crypto.get_hash(message))

    def test_process_challenge_failure_invalid_challenge(self):
        ''' process_challenge should return None if challenge received cannot be decrypted '''
        privkey=crypto.generate_rsa_key()
        challenge=b64encode('challenge'.encode()).decode()
        with self.assertRaises(ValueError) as cm:
            crypto.process_challenge(privkey, challenge)

    def test_generate_rsa_key_success(self):
        ''' generate_rsa_key should return a RSAPrivateKey object with size 4096 '''
        privkey=crypto.generate_rsa_key()
        self.assertTrue(isinstance(privkey, rsa.RSAPrivateKey))
        self.assertEqual(privkey.key_size, 4096)

    def test_store_keys_failure_non_private_key_passed(self):
        ''' store_keys should fail if privkey is not a valid key '''
        privkey='privkey'
        privkey_file='/tmp/test_store_keys_failure_non_private_key_passed.priv'
        pubkey_file='/tmp/test_store_keys_failure_non_private_key_passed.pub'
        with self.assertRaises(AttributeError) as cm:
            crypto.store_keys(privkey, privkey_file, pubkey_file)

    def test_store_keys_failure_no_permission_to_create_private_file(self):
        ''' store_keys should fail if process has no permission to create private file '''
        privkey=crypto.generate_rsa_key()
        privkey_file='/root/test_store_keys_failure_no_permission_to_create_private_file.priv'
        pubkey_file='/tmp/test_store_keys_failure_permission_to_create_private_file.pub'
        with self.assertRaises(OSError) as cm:
            crypto.store_keys(privkey, privkey_file, pubkey_file)
        self.assertEqual(cm.exception.errno, 13)
        self.assertFalse(os.path.isfile(privkey_file))
        self.assertFalse(os.path.isfile(pubkey_file))

    def test_store_keys_failure_no_permission_to_create_public_file(self):
        ''' store_keys should fail if process has no permission to create public file '''
        privkey=crypto.generate_rsa_key()
        privkey_file='/tmp/test_store_keys_failure_permission_to_create_public_file.priv'
        pubkey_file='/root/test_store_keys_failure_permission_to_create_public_file.pub'
        with self.assertRaises(OSError) as cm:
            crypto.store_keys(privkey, privkey_file, pubkey_file)
        self.assertEqual(cm.exception.errno, 13)
        self.assertFalse(os.path.isfile(privkey_file))
        self.assertFalse(os.path.isfile(pubkey_file))

    def test_get_printable_pubkey_failure_invalid_pubkey(self):
        ''' get_printable_pubkey should return None if pubkey is invalid '''
        pubkey='pubkey'
        with self.assertRaises(AttributeError) as cm:
            crypto.get_printable_pubkey(pubkey)

    def test_get_printable_pubkey_success(self):
        ''' get_printable_pubkey should return the string serialization of the public key '''
        privkey=crypto.generate_rsa_key()
        pubkey=privkey.public_key()
        self.assertIsNotNone(crypto.get_printable_pubkey(pubkey))

