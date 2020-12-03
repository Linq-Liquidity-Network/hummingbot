import hmac
import time
import hashlib
from novadax import RequestClient as NovaClient

class NovadaxAuth:
    def __init__(self, novadax_api_key, novadax_secret_key):
        self._api_key = novadax_api_key
        self._secret_key = novadax_secret_key

    def api_key(self):
        return self._api_key

    def secret_key(self):
        return self._secret_key

