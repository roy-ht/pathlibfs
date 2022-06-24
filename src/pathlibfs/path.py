import os
import pathlib
import tempfile
from typing import List, Optional, Union

import fsspec

from . import exception


class PathFs:
    """Wrapping fsspec and add some functionality"""

    def __init__(self, urlpath: str, *, _fs=None, _path: str = "", _chain: str = "", **options):
        """Create PathFs instance

        Args:
            urlpath (str): urlpath which fsspec supports
        """
        if _fs:
            self._fs = _fs
            self._path = _path
            self._chain = _chain
        else:
            fs, path = fsspec.core.url_to_fs(urlpath)
            self._fs: fsspec.AbstractFileSystem = fs
            self._path: str = path

            chains = urlpath.rsplit("::", 1)
            if len(chains) == 1:
                self._chain = ""
            else:
                self._chain = chains[0]
        # normalize local path
        if self._fs.protocol == "file":
            self._path = os.path.normpath(self._path)

    def __truediv__(self, key: str):
        return self.joinpath(key)

    def __eq__(self, other: "PathFs"):
        return self.protocol == other.protocol and self.path == other.path

    def __repr__(self):
        return f"PathFs({self.urlpath})"

    @property
    def protocol(self):
        """Used protocol, such as s3, gcs, file, etc"""
        return self._fs.protocol

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

    @property
    def sep(self):
        """Path separator"""
        return self._fs.sep

    @property
    def parts(self):
        """Return each path "directory", almost same as path.split(sep)"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).parts
        else:
            parts = list(filter(None, self._path.split(self._fs.sep)))
            if self._path.startswith(self._fs.sep):
                return tuple(parts[1:])
            else:
                return tuple(parts)

    @property
    def drive(self):
        """Return drive if path is local, otherwise empty string"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).drive
        else:
            return ""

    @property
    def root(self):
        """Return root if path is local, otherwise empty string"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).root
        else:
            return ""

    @property
    def anchor(self):
        """Same as drive + root"""
        return self.drive + self.root

    @property
    def parents(self):
        """If path has a leading slash, it has "root". otherwise first directory is a root"""
        parents = []
        p = self.parent
        while self.path != p.path:
            parents.append(p)
            p = self.parent
        return parents

    @property
    def parent(self):
        """Parent of the path"""
        p = self._path.rsplit(self.sep, 1)
        if self._path.startswith(self.sep):
            if not p[0]:
                return PathFs("", _fs=self._fs, _path=self._path, _chain=self._chain)
            else:
                return PathFs("", _fs=self._fs, _path=p[0], _chain=self._chain)
        else:
            if len(p) == 1:
                return PathFs("", _fs=self._fs, _path=self._path, _chain=self._chain)
            else:
                return PathFs("", _fs=self._fs, _path=p[0], _chain=self._chain)

    @property
    def has_parent(self):
        """Return if parent exists or not"""
        if self._path.startswith(self._fs.sep):
            return self._path != self._fs.sep
        else:
            return self._fs.sep in self._path

    @property
    def name(self):
        """Return final path component, such as file name"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).name
        else:
            return self._path.rsplit(self.sep, 1)[-1]

    @property
    def suffix(self):
        """Return file extension"""
        if self._fs.protocol == "file":
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
        if self._fs.protocol == "file":
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
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).stem
        else:
            name = self.name
            suffix = self.suffix
            return name[-len(suffix) :]

    def as_posix(self):
        """Same sa standard pathlib for local, otherwise raise an NotImplementedError"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).as_posix()
        else:
            raise NotImplementedError()

    def as_uri(self):
        """Path with protocol, such as file:///etc/passwd"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).as_uri()
        else:
            return self._fs.unstrip_protocol(self._path)

    def is_absolute(self):
        """Return path is absolute for local, remote fs always return True"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).is_absolute()
        else:
            return True

    def is_reserved(self):
        """Same as pathlib for local, remote fs always return False"""
        if self._fs.protocol == "file":
            return pathlib.PurePath(self._path).is_reserved()
        else:
            return False

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
            path = sep + path
        elif not self._path.startswith(sep) and path.startswith(sep):
            path = path.lstrip(sep)
        return PathFs("", _fs=self._fs, _path=path, _chain=self._chain)

    def match(self, pattern: str):
        """Same as pathlib"""
        if self._fs.protocol == "file":
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

    def relative_to(self, p: str) -> str:
        """Return str instead pathlib returns Path instance"""
        if self._fs.protocol == "file":
            return str(pathlib.PurePath(self._path).relative_to(p))
        else:
            pp = self._fs.unstrip_protocol(p)
            fullpath = self._fs.unstrip_protocol(self._path)
            if fullpath.startswith(pp):
                return fullpath[len(pp) :]
            else:
                raise ValueError(f"{self._path} does not start with {p}")

    def with_name(self, name: str) -> "PathFs":
        """Same as pathlib"""
        if self._fs.protocol == "file":
            return PathFs("", _fs=self._fs, _path=str(pathlib.PurePath(self._path).with_name(name)), _chain=self._chain)
        else:
            if not self.name:
                raise ValueError(f"{self._path} has an empty name")
            path = self._path[: -len(self.name)] + name
            return PathFs("", _fs=self._fs, _path=path, _chain=self._chain)

    def with_suffix(self, suffix: str) -> "PathFs":
        """Same as pathlib"""
        if self._fs.protocol == "file":
            return PathFs(
                "", _fs=self._fs, _path=str(pathlib.PurePath(self._path).with_suffix(suffix)), _chain=self._chain
            )
        else:
            path = self._path[: -len(self.suffix)] + suffix
            return PathFs("", _fs=self._fs, _path=path, _chain=self._chain)

    def stat(self, **kwargs):
        """Return fsspec.info. its content varies with underlying filesystem"""
        return self.info(**kwargs)

    def chmod(self, mode: int):
        """Same as pathlib for local, otherwide raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).chmod(mode)
        raise exception.PathFsException("file must be local")

    def glob(self, pattern, **kwargs):
        """Call fsspec's glob API, and returns list of PathFs instance."""
        kwargs.pop("detail", None)
        p = self.joinpath(pattern)._path
        results = self._fs.glob(p, **kwargs)
        return [PathFs("", _fs=self._fs, _path=x, _chain=self._chain) for x in results]

    def group(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).group()
        raise exception.PathFsException("file must be local")

    def is_dir(self):
        """Alias of isdir"""
        return self.isdir()

    def is_file(self):
        """Alias of isfile"""
        return self.isfile()

    def is_mount(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).is_mount()
        raise exception.PathFsException("file must be local")

    def is_symlink(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).is_symlink()
        raise exception.PathFsException("file must be local")

    def is_socket(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).is_socket()
        raise exception.PathFsException("file must be local")

    def is_fifo(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).is_fifo()
        raise exception.PathFsException("file must be local")

    def is_block_device(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).is_block_device()
        raise exception.PathFsException("file must be local")

    def is_char_device(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).is_char_device()
        raise exception.PathFsException("file must be local")

    def iterdir(self, **kwargs):
        """Same as ls, but returns generator"""
        results = self.ls(**kwargs)
        for r in results:
            yield r

    def lchmod(self, mode: int):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).lchmod(mode)
        raise exception.PathFsException("file must be local")

    def mkdir(self, parents: bool = False, exist_ok: bool = False, **kwargs):
        """Make directory"""
        if exist_ok:
            if parents:
                self._fs.makedirs(self._path, exist_ok=True)
            else:
                self._fs.mkdir(self._path, create_parents=False, **kwargs)
        else:
            self._fs.mkdir(self._path, create_parents=parents, **kwargs)

    def open(  # noqa
        self,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ):
        """Open file"""
        return self._fs.open(
            self._path, mode=mode, block_size=block_size, cache_options=cache_options, compression=compression, **kwargs
        )

    def owner(self):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).owner()
        raise exception.PathFsException("file must be local")

    def read_bytes(self):
        """Read raw file contents"""
        return self._fs.cat_file(self._path, mode="rb")

    def read_text(self):
        """Read file contents with text mode"""
        return self._fs.cat_file(self._path, mode="r")

    def rename(self, *args, **kwargs):
        """Alias of mv"""
        return self.replace(*args, **kwargs)

    def replace(self, *args, **kwargs):
        """Alias of mv"""
        return self.mv(*args, **kwargs)

    def resolve(self, strict: bool = False):
        """Same as pathlib for local, otherwise raise an exception"""
        if self.protocol == "file":
            return pathlib.Path(self._path).resolve(strict)
        raise exception.PathFsException("file must be local")

    def rmdir(self):
        """Remove directory"""
        return self._fs.rmdir(self._path)

    def samefile(self, target: Union[str, "PathFs"]):
        """Check target points a same path"""
        if isinstance(target, str):
            target = PathFs(target)
        return self.protocol == target.protocol and self._path == target._path

    def symlink_to(self, target: Union[str, "PathFs"]):
        """Same as pathlib for local, otherwise raise an exception"""
        if isinstance(target, str):
            target = PathFs(target)
        if self.protocol == "file":
            return pathlib.Path(self._path).symlink_to(target.path)
        raise exception.PathFsException("file must be local")

    def touch(self, mode: int = 0o666, exist_ok: bool = True, truncate: bool = False, **kwargs):
        """Create empty file"""
        if not exist_ok and self.exists():
            raise FileExistsError(f"File exists: {self.fullpath}")
        return self._fs.touch(self._path, truncate=truncate, **kwargs)

    def unlink(self):
        """Alias of rm"""
        return self.rm()

    def write_bytes(self, data):
        """Write raw bytes"""
        return self.pipe_file(data)

    def write_text(self, data):
        """Write text"""
        with self.open("w") as f:
            f.write(data)

    def rglob(self, pattern, **kwargs):
        """Glob recursively"""
        return self.joinpath("**").glob(pattern)

    # fsspec methods

    def cat(self, recursive: bool = False, on_error: str = "raise", **kwargs):
        """Same as fsspec's cat. Fetch (potentially multiple) paths' contents"""
        return self._fs.cat(self._path, recursive=recursive, on_error=on_error, **kwargs)

    def checksum(self):
        """Same as fsspec"""
        return self._fs.checksum(self._path)

    def clear_instance_cache(self):
        """Same as fsspec"""
        return self.clear_instance_cache()

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

    def cp(self, *args, **kwargs):
        """Alias of copy"""
        return self.copy(*args, **kwargs)

    def created(self):
        """Same as fsspec"""
        return self._fs.created(self._path)

    def delete(self, *args, **kwargs):
        """Alias of rm"""
        return self.rm(*args, **kwargs)

    def disk_usage(self, *args, **kwargs):
        """Alias of du"""
        return self.du(*args, **kwargs)

    def download(self, *args, **kwargs):
        """Alias of get"""
        return self.get(*args, **kwargs)

    def du(self, total: bool = True, maxdepth: Optional[int] = None, **kwargs):
        """Same as fsspec"""
        return self._fs.du(self._path, total=total, maxdepth=maxdepth, **kwargs)

    def exists(self, **kwargs):
        """Same as fsspec"""
        return self._fs.exists(self._path, **kwargs)

    def expand_path(self, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs) -> List["PathFs"]:
        """Same as fsspec, but returns list of PathFs"""
        results = self._fs.expand_path(self._path, recursive=recursive, maxdepth=maxdepth, **kwargs)
        return [PathFs("", _fs=self._fs, _path=x, _chain=self._chain) for x in results]

    def find(self, maxdepth: Optional[int] = None, withdirs: bool = False, **kwargs):
        """Same as fsspec, but returns list of PathFs"""
        kwargs.pop("detail", None)
        results = self._fs.find(self._path, maxdepth=maxdepth, withdirs=withdirs, **kwargs)
        return [PathFs("", _fs=self._fs, _path=x, _chain=self._chain) for x in results]

    def get(
        self, path: Union[str, "PathFs"], recursive: bool = False, callback=fsspec.callbacks._DEFAULT_CALLBACK, **kwargs
    ):
        """Same as fsspec"""
        target = PathFs(path) if isinstance(path, str) else path
        if target.protocol != "file":
            raise exception.PathFsException("path must be a local filesystem")
        return self._fs.get(self._path, path, recursive=recursive, callback=callback, **kwargs)

    def head(self, size: int = 1024):
        """Same as fsspec"""
        return self._fs.head(self._path, size=size)

    def info(self, **kwargs):
        """Same as fsspec"""
        return self._fs.info(**kwargs)

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

    def listdir(self, **kwargs):
        """Alias of ls"""
        return self.ls()

    def ls(self, **kwargs):
        """Same as fsspec, but returns list of PathFs"""
        kwargs.pop("detail", None)
        results = [PathFs("", _fs=self._fs, _path=x, _chain=self._chain) for x in self._fs.ls(self._path, **kwargs)]
        return [x for x in results if x != self]

    def makedir(self, *args, **kwargs):
        """Alias of mkdir"""
        return self.mkdir(*args, **kwargs)

    def makedirs(self, exist_ok: bool = False, **kwargs):
        """Alias of mkdir with making parent dirs"""
        return self.mkdir(parents=True, exist_ok=exist_ok, **kwargs)

    def mkdirs(self, *args, **kwargs):
        """Alias of makedirs"""
        return self.makedirs(*args, **kwargs)

    def modified(self):
        """Same as fsspec"""
        return self._fs.modified(self._path)

    def move(self, *args, **kwargs):
        """Alias of mv"""
        return self.mv(*args, **kwargs)

    def mv(self, target: Union[str, "PathFs"], recursive=False, maxdepth=None, **kwargs):
        """Same as fsspec"""
        if isinstance(target, str):
            target = PathFs(target)
        if self.protocol != target.protocol:
            raise exception.PathFsException("target must be same protocol")
        return self._fs.mv(self._path, target.path, recursive=recursive, maxdepth=maxdepth, **kwargs)

    def pipe_file(self, data):
        """Same as fsspec"""
        return self._fs.pipe_file(self._path, data)

    def put(
        self, path: Union[str, "PathFs"], recursive: bool = False, callback=fsspec.callbacks._DEFAULT_CALLBACK, **kwargs
    ):
        """Same as fsspec"""
        target = PathFs(path) if isinstance(path, str) else path
        if target.protocol != "file":
            raise exception.PathFsException("path must be a local filesystem")
        return self._fs.put(target.path, self.path, recursive=recursive, callback=callback, **kwargs)

    def read_block(self, offset: int, length: int, delimiter: Optional[bytes] = None):
        """Same as fsspec"""
        return self._fs.read_block(self._path, offset=offset, length=length, delimiter=delimiter)

    def rm(self, recursive: bool = False, maxdepth: Optional[int] = None):
        """Same as fsspec"""
        return self._fs.rm(self._path, recursive=recursive, maxdepth=maxdepth)

    def rm_file(self):
        """Same as fsspec"""
        return self._fs.rm_file(self._path)

    def sign(self, expiration: int = 100, **kwargs):
        """Same as fsspec"""
        return self._fs.sign(self._path, expiration=expiration, **kwargs)

    def size(self):
        """Same as fsspec"""
        return self._fs.size(self._path)

    def tail(self, size: int = 1024):
        """Same as fsspec"""
        return self._fs.tail(size)

    def ukey(self):
        """Same as fsspec"""
        return self._fs.ukey(self._path)

    def upload(self, *args, **kwargs):
        """Alias of put"""
        return self.put(*args, **kwargs)

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
            yield PathFs("", _fs=self._fs, _path=path, _chain=self._chain), dirs, files
