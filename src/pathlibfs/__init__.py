import os
import warnings

from .exception import PathlibfsException
from .path import Path

if "PATHLIBFS_S3_SESSION_CACHE" in os.environ:
    try:
        from . import s3_support

        s3_support.register_session_cache()
    except ImportError as e:  # pragma: no cover
        warnings.warn(f"PATHLIBFS_S3_SESSION_CACHE found, but failed: {e}")


__all__ = ["Path", "PathlibfsException"]
