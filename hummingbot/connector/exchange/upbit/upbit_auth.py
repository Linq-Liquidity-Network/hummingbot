from typing import (
    # Optional,
    Dict,
    Any
)
import uuid
import jwt
import hashlib
from urllib.parse import urlencode


class UpbitAuth:
    def __init__(self, api_key: str, secret_key: str):
        self._api_key = api_key
        self._secret_key = secret_key

    @property
    def api_key(self):
        return self._api_key

    def sign_request(self, query = None):
        if query is not None:
            m = hashlib.sha512()
            m.update(urlencode(query).encode())
            query_hash = m.hexdigest()

            payload = {
                'access_key': self.api_key,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }
        else:
            payload = {
                'access_key': self.api_key,
                'nonce': str(uuid.uuid4()),
            }

        jwt_token = jwt.encode(payload, self._secret_key).decode('utf8')
        authorization_token = 'Bearer {}'.format(jwt_token)
        return payload, authorization_token
