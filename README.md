# pathlibfs

[![codecov](https://codecov.io/gh/roy-ht/pathlibfs/branch/main/graph/badge.svg?token=MX1DTY2CNG)](https://codecov.io/gh/roy-ht/pathlibfs)

pathlib 🤝 fsspec

Like pathlib, Python standard library module, manipulate remote filesystem paths.

# Installation

```
pip install pathlibfs
```

# Getting Started

It only provide `Path` class:

```python
from pathlibfs import Path

p = Path('your/path.txt')
p_s3 = Path('s3://bucket/key.txt')
p_gcs = Path('gs://bucket/key.txt')
```

pathlibfs uses [fsspec](https://github.com/fsspec/filesystem_spec) as a backend filesystem.
So if you want to use some specific remote filesystem, you need to install extra dependencies such as `s3fs` or `gcsfs`.

See [known implementations](https://github.com/fsspec/filesystem_spec/blob/a8cfd9c52a20c930c67ff296b60dbcda89d64db9/fsspec/registry.py#L87)
to check out supported protocols.

# Special Environment Variables
| name | description |
|-|-|
| PATHLIBFS_S3_SESSION_CACHE | If defined, store S3 session cache locally like [boto3-session-cache](https://github.com/mixja/boto3-session-cache) |

# APIs

`Path` class has many methods, and it's almost same as [pathlib](https://docs.python.org/3.10/library/pathlib.html) and [fsspec.AbstractFileSystem](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem)
.

For example, like pathlib, you can join path with `/`:

```
p = Path('some/dir') / 'subdir'  # -> Path('some/dir/subdir')
```

## properties

Gotcha.

| name | description |
|-|-|
| fs | backend filesystem of fsspec |
| path | path without protocol. e.g. Path('s3://bucket/key') == 'bucket/key' |
| fullpath | path with protocol. e.g. Path('a.txt') == 'file://a.txt' |
| urlpath | path with full chain. e.g. Path('simplecache::s3://bucket/key') == 'simplecache::s3://bucket/key' |
| drive | same as pathlib |
| root | same as pathlib |
| parts | same as pathlib |
| anchor | same as pathlib |
| parents | same as pathlib, return List[Path] |
| parent | same as pathlib, return Path |
| has_parent | same as `path.parent != path` |
| name | same as pathlib |
| suffix | same as pathlib |
| suffixes | same as pathlib |
| stem | same as pathlib |
| sep | separator of backend filesystem, such as '/' |
| protocol | backend protocol. e.g. 's3', 'gcs' |


## pathlib based operations

| name | description |
|-|-|
| `as_posix()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `chmod(mode: int)` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `group()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `is_mount()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `is_symlink()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `is_socket()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `is_fifo()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `is_block_device()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `is_char_device()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `owner()` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `symlink_to(target: PathLike)` | only for local filesystem, otherwise raise an `PathlibfsException` |
| `resolve()` | only for local filesystem, otherwise return `self` |
| `is_absolute()` | only for local filesystem, otherwise return True |
| `is_reserved()` | only for local filesystem, otherwise return False |
| `joinpath(*p)` | same as pathlib |
| `match(pattern: str)` | same as pathlib |
| `with_name(name: str)` | same as pathlib |
| `with_suffix(suffix: str)` | same as pathlib |
| `read_bytes()` | same as pathlib |
| `read_text()` | same as pathlib |
| `write_text(data)` | same as pathlib |
| `write_bytes(data)` | same as pathlib |
| `is_dir()` | same as pathlib |
| `is_file()` | same as pathlib |
| `unlink()` | same as pathlib |
| `relative_to(path: PathLike)` | same as pathlib, but **return str**. `path == other / path.relative_to(other)` |
| `iterdir(**kwargs)` | same as pathlib. It's just an wrapper of ls(), **it's not efficient.** Use `ls()`. |
| `stat()` | alias of info. **It's not same as pathlib**, but fsspec's `info()` |
| `rglob(pattern, **kwargs)` | same meaning as pathlib, and accept fsspec's `glob()` |

## fsspec based mathods

| name | description |
|-|-|
| `ls(**kwargs)` | same as fsspec |
| `listdir()` | alias of ls |
| `find(maxdepth: Optional[int] = None, withdirs: bool = False, **kwargs)` | same as fsspec |
| `glob(pattern, **kwargs)` | same as fsspec |
| `expand_path(recursive: bool = False, maxdepth: Optional[int] = None, **kwargs)` | same as fsspec |
| `walk(maxdepth: Optional[int] = None, **kwargs)` | same as `os.walk` and fsspec's `walk`, yield `(dirpath, dirnames, filenames)`. And `dirpath` is `Path` instance. |
| `exists(**kwargs)` | same as fsspec |
| `isdir()` | same as fsspec |
| `isfile()` | same as fsspec |
| `lexists()` | same as fsspec |
| `ukey()` | same as fsspec |
| `checksum()` | same as fsspec |
| `sign(expiration: int = 100, **kwargs)` | same as fsspec |
| `size()` | same as fsspec |
| `created()` | same as fsspec |
| `modified()` | same as fsspec |
| `du(total: bool = True, maxdepth: Optional[int] = None, **kwargs)` | same as fsspec |
| `disk_usage()` | alias of du |
| `info(**kwargs)` | same as fsspec |
| `open()` | same as fsspec |
| `cat(recursive: bool = False, on_error: str = "raise", **kwargs)` | same as fsspec |
| `read_block(offset: int, length: int, delimiter: Optional[bytes] = None)` | same as fsspec |
| `head(size: int = 1024)` | same as fsspec |
| `tail(size: int = 1024)` | same as fsspec |
| `mkdir(parents: bool = False, exist_ok: bool = False, **kwargs)` | same as fsspec |
| `makedir()` | alias of mkdir |
| `makedirs(exist_ok: bool = False, **kwargs)` | same as `mkdir(parents=True)` |
| `mkdirs()` | alias of makedirs |
| `touch(mode: int = 0o666, exist_ok: bool = True, truncate: bool = False, **kwargs) | same as fsspec |
| `pipe_file(data)` | same as fsspec |
| `rm_file()` | same as fsspec |
| `rm(recursive: bool = False, maxdepth: Optional[int] = None)` | same as fsspec |
| `delete()` | alias of rm |
| `invalidate_cache()` | same as fsspec |
| `clear_instance_cache()` | same as fsspec |
| `copy(dst: PathLike, recursive: bool = False, on_error: Optional[str] = None, **kwargs)` | copy the path to dst. `copy()` can handle any protocol combinations so you don't need to call `put()` or `get()` for almost all cases. |
| `cp()` | alias of copy |
| `move(dst: PathLike, recursive=False, maxdepth=None, **kwargs)` | similar to copy, but delete source path after copy. |
| `mv()`, `rename()`, `replace() | alias of move |
| `put(target: PathLike, recursive: bool = False, callback=fsspec.callbacks._DEFAULT_CALLBACK, **kwargs)` | Upload **local** target to the path. |
| `upload()` | alias of put |
| `get(arget: PathLike, recursive: bool = False, callback=fsspec.callbacks._DEFAULT_CALLBACK, **kwargs)` | Downalod the path into **local** target. |
| `download()` | alias of get |

## others

| name | description |
|-|-|
| `islocal()` | True if protocol is local filesystem. |
| `clone(path: Optional[str] = None)` | copy self instance with different path (optional). |
| `samefile(target: PathLike)` | same as `self == target` |


# How to test

Start mock server for testing.

```
docker-compose up -d
```

Run test:
```
pytest
```