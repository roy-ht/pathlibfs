from typing import Tuple

import fsspec

from . import exception


class PathFs:
    def __init__(self, urlpath: str, **options):
        """Create PathFs instance

        Args:
            p (str): urlpath
        """
        self._original_url = urlpath
        fs, path = fsspec.core.url_to_fs(urlpath)
        self._fs: fsspec.AbstractFileSystem = fs
        self._path: str = path

    def joinpath(self, *p):
        """self._path might have leading slash /, so it preserve this condition"""
        # below code is based on CPython's os.path.join
        sep = self._fs.sep
        path = self._path
        for b in p:
            if b.startswith(sep):
                path = b
            elif not path or path.endswith(sep):
                path += b
            else:
                path += sep + b
        if self._path.startswith(sep) and not path.startswith(sep):
            self._path = sep + path
        elif not self._path.startswith(sep) and path.startswith(sep):
            self._path = path.lstrip(sep)
        return self


