"""Support code for s3 filesystem"""

import copy
import multiprocessing
import threading

import aiobotocore.session
import botocore.credentials


class CredentialCache(botocore.credentials.JSONFileCache):
    """Json file cache for multiprocessing, and in-memory cache for threads."""

    _cache_storage = {}

    def __init__(self, *args, **kwargs):
        """Wrapper init"""
        self._plock = multiprocessing.Lock()
        self._tlock = threading.Lock()
        CredentialCache._cache_storage = {}
        super().__init__(*args, **kwargs)

    def __contains__(self, cache_key):
        with self._tlock:
            if cache_key in CredentialCache._cache_storage:
                return True
        with self._plock:
            return super().__contains__(cache_key)

    def __getitem__(self, cache_key):
        with self._tlock:
            if cache_key in CredentialCache._cache_storage:
                return copy.deepcopy(CredentialCache._cache_storage[cache_key])
        with self._plock:
            return super().__getitem__(cache_key)

    def __delitem__(self, cache_key):
        with self._tlock:
            if cache_key in CredentialCache._cache_storage:
                del CredentialCache._cache_storage[cache_key]
        with self._plock:
            return super().__delitem__(cache_key)

    def __setitem__(self, cache_key, value):
        with self._tlock:
            CredentialCache._cache_storage[cache_key] = value
        with self._plock:
            super().__setitem__(cache_key, value)


def register_session_cache():
    orig_resolver = aiobotocore.session.create_credential_resolver

    def _patch(*args, **kwargs):
        """Monkey patching to aiobotocore.session.credential_resolver"""
        kwargs["cache"] = CredentialCache()
        return orig_resolver(*args, **kwargs)

    aiobotocore.session.create_credential_resolver = _patch
