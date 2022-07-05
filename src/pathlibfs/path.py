import copy
import datetime
import functools
import os
import pathlib
import tempfile
from typing import IO, Any, List, Optional, Union

import fsspec

from . import exception


def only_local(otherwise: Any = None):
    def deco(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            if self.protocol == "file":
                return f(self, *args, **kwargs)
            elif otherwise is not None:
                return otherwise
            else:
                raise exception.PathFsException("file must be local")

        return wrapper

    return deco


class PathFs:
    """Wrapping fsspec and add some functionality"""

    def __init__(self, urlpath: Any, **options):
        """Create PathFs instance

        Args:
            urlpath (str): urlpath which fsspec supports
        """
        if isinstance(urlpath, PathFs):
            spath = urlpath.urlpath
        # Support PathLike object
        elif hasattr(urlpath, "__fspath__") and callable(urlpath.__fspath__):
            spath: str = str(urlpath.__fspath__())
        else:
            spath: str = urlpath
        fs, path = fsspec.core.url_to_fs(spath, **options)
        self._fs: fsspec.AbstractFileSystem = fs
        self._path: str = path
        chains = spath.rsplit("::", 1)
        if len(chains) == 1:
            self._chain = ""
        else:
            self._chain = chains[0]
        # determin protocol
        if isinstance(self._fs.protocol, (list, tuple)):
            for prt in self._fs.protocol:
                if f"{prt}://" in spath:
                    self._protocol = prt
                    break
            else:
                self._protocol = self._fs.protocol[0]
        else:
            self._protocol = self._fs.protocol

        # normalize local path
        if self.protocol == "file":
            self._path = os.path.normpath(self._path)

    def __truediv__(self, key: str):
        return self.joinpath(key)

    def __eq__(self, other: "PathFs"):
        return self.protocol == other.protocol and self.path == other.path

    def __repr__(self):
        return f"PathFs({self.urlpath})"

    # ------------------------------- Wrapper of pathlib

    @property
    def sep(self):
        """Path separator"""
        return self._fs.sep

    @property
    @only_local("")
    def drive(self) -> str:
        """Return drive if path is local, otherwise empty string"""
        return pathlib.PurePath(self._path).drive

    @property
    @only_local("")
    def root(self) -> str:
        """Return root if path is local, otherwise empty string"""
        return pathlib.PurePath(self._path).root

    @only_local()
    def as_posix(self):
        """Same sa standard pathlib for local, otherwise raise an NotImplementedError"""
        return pathlib.PurePath(self._path).as_posix()

    @only_local()
    def chmod(self, mode: int, *, dir_fd=None, follow_symlinks=True):
        """Same as pathlib for local, otherwide raise an exception"""
        return os.chmod(self._path, mode, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    @only_local()
    def group(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).group()

    @only_local()
    def is_mount(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).is_mount()

    @only_local()
    def is_symlink(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).is_symlink()

    @only_local()
    def is_socket(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).is_socket()

    @only_local()
    def is_fifo(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).is_fifo()

    @only_local()
    def is_block_device(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).is_block_device()

    @only_local()
    def is_char_device(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).is_char_device()

    @only_local()
    def owner(self):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).owner()

    @only_local(True)
    def is_absolute(self):
        """Return path is absolute for local, remote fs always return True"""
        return pathlib.PurePath(self._path).is_absolute()

    @only_local(False)
    def is_reserved(self):
        """Same as pathlib for local, remote fs always return False"""
        return pathlib.PurePath(self._path).is_reserved()

    @only_local()
    def resolve(self, strict: bool = False):
        """Same as pathlib for local, otherwise raise an exception"""
        return pathlib.Path(self._path).resolve(strict)

    @only_local()
    def symlink_to(self, target: Union[str, "PathFs"]):
        """Same as pathlib for local, otherwise raise an exception"""
        if isinstance(target, str):
            target = PathFs(target)
        return pathlib.Path(self._path).symlink_to(target.path)

    @property
    def parts(self):
        """Return each path "directory", almost same as path.split(sep)"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).parts
        else:
            parts = list(filter(None, self._path.split(self.sep)))
            if self._path.startswith(self.sep):
                return tuple(parts[1:])
            else:
                return tuple(parts)

    @property
    def anchor(self):
        """Same as drive + root"""
        return self.drive + self.root

    @property
    def parents(self) -> List["PathFs"]:
        """Return all parents"""
        if self == self.parent:
            return []
        else:
            return [self.parent] + self.parent.parents

    @property
    def parent(self) -> "PathFs":
        """Parent of the path"""
        if not hasattr(self, "__parent"):
            norm_path = self.path.rstrip(self.sep)
            if not norm_path:  # ex: /
                pp = self
            else:
                p = norm_path.rsplit(self.sep, 1)
                if not p[0]:  # ex: /etc
                    if self.protocol == "file":
                        pp = self.clone(self.sep)
                    else:
                        pp = self
                else:
                    pp = self.clone(p[0])
            self.__parent = pp
        return self.__parent

    @property
    def has_parent(self):
        """Return if parent exists or not"""
        if self._path.startswith(self.sep):
            return self._path != self.sep
        else:
            return self.sep in self._path

    @property
    def name(self):
        """Return final path component, such as file name"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).name
        else:
            return self._path.rsplit(self.sep, 1)[-1]

    @property
    def suffix(self):
        """Return file extension"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).suffix
        else:
            exts = self.name.rsplit(".", 1)
            if len(exts) == 1:
                return ""
            else:
                return "." + exts[-1]

    @property
    def suffixes(self):
        """Return file extensions, such as ['.tar', '.gz']"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).suffixes
        else:
            exts = self.name.rsplit(".")
            if len(exts) == 1:
                return []
            else:
                return ["." + x for x in exts]

    @property
    def stem(self):
        """Same as name - suffix"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).stem
        else:
            name = self.name
            suffix = self.suffix
            return name[-len(suffix) :]

    def as_uri(self):
        """Path with protocol, such as file:///etc/passwd"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).as_uri()
        else:
            return self._fs.unstrip_protocol(self._path)

    def joinpath(self, *p):
        """self._path might have leading slash /, so it preserve this condition"""
        # below code is based on CPython's os.path.join
        sp = self.sep
        path = self._path
        for b in p:
            if b.startswith(sp):
                path = b
            elif not path or path.endswith(sp):
                path += b
            else:
                path += sp + b
        if self._path.startswith(sp) and not path.startswith(sp):
            path = sp + path
        elif not self._path.startswith(sp) and path.startswith(sp):
            path = path.lstrip(sp)
        return self.clone(path)

    def match(self, pattern: str):
        """Same as pathlib"""
        if self.protocol == "file":
            return pathlib.PurePath(self._path).match(pattern)
        else:
            # if separator is not a slash, convert temporary
            path = self._path
            if self.sep != "/":
                path = path.replace("/", "\tslash\t")
                path = path.replace(self.sep, "/")
                pattern = pattern.replace("/", "\tslash\t")
                pattern = pattern.replace(self.sep, "/")
            if not path.startswith("/"):
                # always absolute
                path = "/" + path

            return pathlib.PurePath(path).match(pattern)

    def with_name(self, name: str) -> "PathFs":
        """Same as pathlib"""
        if self.protocol == "file":
            return self.clone(str(pathlib.PurePath(self._path).with_name(name)))
        else:
            if not self.name:
                raise ValueError(f"{self._path} has an empty name")
            path = self._path[: -len(self.name)] + name
            return self.clone(path)

    def with_suffix(self, suffix: str) -> "PathFs":
        """Same as pathlib"""
        if self.protocol == "file":
            return self.clone(str(pathlib.PurePath(self._path).with_suffix(suffix)))
        else:
            path = self._path[: -len(self.suffix)] + suffix
            return self.clone(path)

    # ------------------------------- Wrapper of fsspec

    @property
    def protocol(self):
        """Used protocol, such as s3, gcs, file, etc"""
        return self._protocol

    def glob(self, pattern, **kwargs):
        """Call fsspec's glob API, and returns list of PathFs instance."""
        kwargs.pop("detail", None)
        p = self.joinpath(pattern)._path
        results = self._fs.glob(p, **kwargs)
        return [self.clone(x) for x in results]

    def open(  # noqa
        self,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> IO:
        """Open file"""
        return self._fs.open(
            self._path, mode=mode, block_size=block_size, cache_options=cache_options, compression=compression, **kwargs
        )

    def ukey(self):
        """Same as fsspec"""
        return self._fs.ukey(self._path)

    def tail(self, size: int = 1024) -> bytes:
        """Same as fsspec"""
        return self._fs.tail(self._path, size)

    def rm_file(self):
        """Same as fsspec"""
        return self._fs.rm_file(self._path)

    def sign(self, expiration: int = 100, **kwargs):
        """Same as fsspec"""
        return self._fs.sign(self._path, expiration=expiration, **kwargs)

    def size(self):
        """Same as fsspec"""
        return self._fs.size(self._path)

    def read_block(self, offset: int, length: int, delimiter: Optional[bytes] = None) -> bytes:
        """Same as fsspec"""
        return self._fs.read_block(self._path, offset=offset, length=length, delimiter=delimiter)

    def rm(self, recursive: bool = False, maxdepth: Optional[int] = None):
        """Same as fsspec"""
        return self._fs.rm(self._path, recursive=recursive, maxdepth=maxdepth)

    def put(
        self, path: Union[str, "PathFs"], recursive: bool = False, callback=fsspec.callbacks._DEFAULT_CALLBACK, **kwargs
    ):
        """Same as fsspec"""
        target = PathFs(path) if isinstance(path, str) else path
        if target.protocol != "file":
            raise exception.PathFsException("path must be a local filesystem")
        return self._fs.put(target.path, self.path, recursive=recursive, callback=callback, **kwargs)

    def pipe_file(self, data):
        """Same as fsspec"""
        return self._fs.pipe_file(self._path, data)

    def mv(self, target: Union[str, "PathFs"], recursive=False, maxdepth=None, **kwargs):
        """Same as fsspec"""
        if isinstance(target, str):
            target = PathFs(target)
        if self.protocol != target.protocol:
            raise exception.PathFsException("target must be same protocol")
        return self._fs.mv(self._path, target.path, recursive=recursive, maxdepth=maxdepth, **kwargs)

    def modified(self) -> datetime.datetime:
        """Same as fsspec"""
        return self._fs.modified(self._path)

    def ls(self, **kwargs):
        """Same as fsspec, but returns list of PathFs"""
        kwargs.pop("detail", None)
        results = [self.clone(x) for x in self._fs.ls(self._path, **kwargs)]
        return [x for x in results if x != self]

    def head(self, size: int = 1024):
        """Same as fsspec"""
        return self._fs.head(self._path, size=size)

    def info(self, **kwargs):
        """Same as fsspec"""
        return self._fs.info(self._path, **kwargs)

    def invalidate_cache(self):
        """Same as fsspec"""
        return self._fs.invalidate_cache(self._path)

    def isdir(self):
        """Same as fsspec"""
        return self._fs.isdir(self._path)

    def isfile(self):
        """Same as fsspec"""
        return self._fs.isfile(self._path)

    def lexists(self):
        """Same as fsspec"""
        return self._fs.lexists(self._path)

    def get(
        self, path: Union[str, "PathFs"], recursive: bool = False, callback=fsspec.callbacks._DEFAULT_CALLBACK, **kwargs
    ):
        """Same as fsspec"""
        target = PathFs(path) if isinstance(path, str) else path
        if target.protocol != "file":
            raise exception.PathFsException("path must be a local filesystem")
        return self._fs.get(self._path, path, recursive=recursive, callback=callback, **kwargs)

    def find(self, maxdepth: Optional[int] = None, withdirs: bool = False, **kwargs):
        """Same as fsspec, but returns list of PathFs"""
        kwargs.pop("detail", None)
        results = self._fs.find(self._path, maxdepth=maxdepth, withdirs=withdirs, **kwargs)
        return [self.clone(x) for x in results]

    def expand_path(self, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs) -> List["PathFs"]:
        """Same as fsspec, but returns list of PathFs"""
        results = self._fs.expand_path(self._path, recursive=recursive, maxdepth=maxdepth, **kwargs)
        return [self.clone(x) for x in results]

    def du(self, total: bool = True, maxdepth: Optional[int] = None, **kwargs):
        """Same as fsspec"""
        return self._fs.du(self._path, total=total, maxdepth=maxdepth, **kwargs)

    def exists(self, **kwargs):
        """Same as fsspec"""
        return self._fs.exists(self._path, **kwargs)

    def created(self) -> datetime.datetime:
        """Same as fsspec"""
        return self._fs.created(self._path)

    def cat(self, recursive: bool = False, on_error: str = "raise", **kwargs):
        """Same as fsspec's cat. Fetch (potentially multiple) paths' contents"""
        return self._fs.cat(self._path, recursive=recursive, on_error=on_error, **kwargs)

    def checksum(self):
        """Same as fsspec"""
        return self._fs.checksum(self._path)

    def clear_instance_cache(self):
        """Same as fsspec"""
        return self._fs.clear_instance_cache()

    # ------------------------------- Other

    @property
    def fs(self):
        """Underlying fsspec filesystem"""
        return self._fs

    @property
    def path(self):
        """Path part of the protocol

        If protocol is file (Local file system), path part always be absolute, otherwise depends on its protocol
        """
        return self._path

    @property
    def fullpath(self):
        """Path with protocol like file://a/b/c.txt or s3://mybucket/some/file.txt"""
        return self._fs.unstrip_protocol(self._path)

    @property
    def urlpath(self):
        """Full path with url chain, like simplecache::s3://some/file.txt"""
        if self._chain:
            return f"{self._chain}::{self.fullpath}"
        else:
            return self.fullpath

    def relative_to(self, p: str) -> str:
        """Return str instead pathlib returns Path instance"""
        if self.protocol == "file":
            return str(pathlib.PurePath(self._path).relative_to(p))
        else:
            pp = self._fs.unstrip_protocol(p)
            fullpath = self._fs.unstrip_protocol(self._path)
            if fullpath.startswith(pp):
                return fullpath[len(pp) :]
            else:
                raise ValueError(f"{self._path} does not start with {p}")

    def iterdir(self, **kwargs):
        """Same as ls, but returns generator"""
        results = self.ls(**kwargs)
        for r in results:
            yield r

    def mkdir(self, parents: bool = False, exist_ok: bool = False, **kwargs):
        """Make directory"""
        if exist_ok:
            if parents:
                self._fs.makedirs(self._path, exist_ok=True)
            else:
                self._fs.mkdir(self._path, create_parents=False, **kwargs)
        else:
            self._fs.mkdir(self._path, create_parents=parents, **kwargs)

    def read_bytes(self):
        """Read raw file contents"""
        return self._fs.cat_file(self._path, mode="rb")

    def read_text(self):
        """Read file contents with text mode"""
        return self._fs.cat_file(self._path, mode="r")

    def rmdir(self):
        """Remove directory"""
        return self._fs.rmdir(self._path)

    def samefile(self, target: Union[str, "PathFs"]):
        """Check target points a same path"""
        if isinstance(target, str):
            target = PathFs(target)
        return self == target

    def touch(self, mode: int = 0o666, exist_ok: bool = True, truncate: bool = False, **kwargs):
        """Create empty file"""
        if not exist_ok and self.exists():
            raise FileExistsError(f"File exists: {self.fullpath}")
        return self._fs.touch(self._path, truncate=truncate, **kwargs)

    def write_text(self, data):
        """Write text"""
        with self.open("w") as f:
            f.write(data)

    def rglob(self, pattern, **kwargs):
        """Glob recursively"""
        return self.joinpath("**").glob(pattern)

    def copy(self, dst: Union[str, "PathFs"], recursive: bool = False, on_error: Optional[str] = None, **kwargs):
        """If dst is str, Same bihavior as fsspec.

        If dst is PathFs instance somethind different:
        * self is local and dst is not: put
        * dst is local and self is not: get
        * otherwise get and put
        """
        if isinstance(dst, PathFs):
            if self.protocol != "file" and dst.protocol == "file":  # remote -> local
                self._fs.get(self._path, dst.path, recursive=recursive, **kwargs)
            elif self.protocol == "file" and dst.protocol != "file":  # local -> remote
                dst.put(self._path)
            elif self.protocol == dst.protocol:  # just copy
                self._fs.copy(self._path, dst._path, recursive=recursive, on_error=on_error, **kwargs)
            else:  # remote -> remote
                # TODO: copy stream
                with tempfile.TemporaryDirectory() as tdir:
                    self._fs.get(self._path, tdir, recursive=recursive, **kwargs)
                    dst.put(tdir, recursive=recursive, **kwargs)
        else:
            self._fs.copy(self._path, dst, recursive=recursive, on_error=on_error, **kwargs)

    def makedirs(self, exist_ok: bool = False, **kwargs):
        """Alias of mkdir with making parent dirs"""
        return self.mkdir(parents=True, exist_ok=exist_ok, **kwargs)

    def walk(self, maxdepth: Optional[int] = None, **kwargs):
        """Like os.walk.

        To get each dir/path, join dirs/files.

        for rootpath, dirs, files in PathFs('s3://some').walk():
            for name in files:  # each file path
                path = rootpath / name
            for name in dirs:
                dr = rootpath / name
        """
        kwargs.pop("detail", None)
        for path, dirs, files in self._fs.walk(self._path, maxdepth=maxdepth, **kwargs):
            # dirs and files
            yield self.clone(path), dirs, files

    # ------------------------------- Alias methods

    def stat(self, **kwargs):
        """Alias of info"""
        return self.info(**kwargs)

    def is_dir(self):
        """Alias of isdir"""
        return self.isdir()

    def is_file(self):
        """Alias of isfile"""
        return self.isfile()

    def unlink(self):
        """Alias of rm"""
        return self.rm()

    def cp(self, *args, **kwargs):
        """Alias of copy"""
        return self.copy(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """Alias of rm"""
        return self.rm(*args, **kwargs)

    def disk_usage(self, *args, **kwargs):
        """Alias of du"""
        return self.du(*args, **kwargs)

    def download(self, *args, **kwargs):
        """Alias of get"""
        return self.get(*args, **kwargs)

    def listdir(self, **kwargs):
        """Alias of ls"""
        return self.ls(**kwargs)

    def makedir(self, *args, **kwargs):
        """Alias of mkdir"""
        return self.mkdir(*args, **kwargs)

    def mkdirs(self, *args, **kwargs):
        """Alias of makedirs"""
        return self.makedirs(*args, **kwargs)

    def upload(self, *args, **kwargs):
        """Alias of put"""
        return self.put(*args, **kwargs)

    def move(self, *args, **kwargs):
        """Alias of mv"""
        return self.mv(*args, **kwargs)

    def rename(self, *args, **kwargs):
        """Alias of mv"""
        return self.replace(*args, **kwargs)

    def replace(self, *args, **kwargs):
        """Alias of mv"""
        return self.mv(*args, **kwargs)

    def write_bytes(self, *args, **kwargs):
        """Alias of pipe_file"""
        return self.pipe_file(*args, **kwargs)

    # ------------------------------- Original Extension

    def islocal(self):
        return self.protocol == "file"

    def clone(self, path: Optional[str] = None):
        """Copy instance with optional different path"""
        # Maybe okey with shallow copy
        other = copy.copy(self)
        if path is not None:
            other._path = path
        return other
