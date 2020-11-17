from typing import (
    # Optional,
    Dict,
    Any
)
import hmac
import time
import hashlib


class BitbayAuth:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def generate_auth_dict(self, payload: str = "") -> Dict[str, Any]:
        """
        Generates authentication signature and returns it in a dictionary
        :return: a dictionary of request info including the request signature and post data
        """
        ts = self.make_nonce()
        sig = self.auth_sig(ts, payload)

        return {
            "publicKey": self.api_key,
            "hashSignature": sig,
            "requestTimestamp": ts
        }

    def make_nonce(self) -> str:
        return str(round(time.time() * 1000))

    def auth_sig(self, nonce: str, payload: str) -> str:
        sig = hmac.new(
            self.secret_key.encode('utf8'),
            '{}{}'.format(self.api_key,nonce, payload).encode('utf8'),
            hashlib.sha512
        ).hexdigest()
        return sig
