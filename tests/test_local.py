"""Test Local filesystem

Check wrapping methods as simple as possible
"""

import datetime
import pathlib
import time

import pytest
from fsspec.implementations.local import LocalFileSystem

from pathlibfs import PathFs

# Just wraps pathlib if local filesystem ---------------------------


def test_drive(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.drive == p.drive


def test_root(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.root == p.root


def test_as_posix(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.as_posix() == p.as_posix()


def test_chmod(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    p = PathFs(a)
    p.chmod(0o775)
    assert a.stat().st_mode == 0o100775


def test_group(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.group() == p.group()


def test_is_mount(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_mount() == p.is_mount()


def test_is_symlink(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_symlink() == p.is_symlink()
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert PathFs(b).is_symlink()


def test_is_socket(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_socket() == p.is_socket()


def test_is_fifo(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_fifo() == p.is_fifo()


def test_is_block_device(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_block_device() == p.is_block_device()


def test_is_char_device(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_char_device() == p.is_char_device()


def test_owner(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.owner() == p.owner()


def test_is_absolute(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_absolute() == p.is_absolute()
    pp = PathFs("a.txt")
    assert pp.is_absolute()  # local filesystem always absolute


def test_is_reserved(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.is_reserved() == p.is_reserved()


def test_resolve(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    p = PathFs(b)
    pp = p.resolve()
    assert pp.name == "a.txt"


def test_symlink_to(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert b.is_symlink()


def test_parts(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.parts == p.parts


def test_name(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.name == p.name


def test_suffix(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = PathFs(a)
    assert p.suffix == ".gz"


def test_suffixes(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = PathFs(a)
    assert p.suffixes == [".tar", ".gz"]


def test_stem(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = PathFs(a)
    assert p.stem == "a.tar"


def test_as_uri(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    assert tmp_path.as_uri() == p.as_uri()


def test_match():
    a = pathlib.Path("a/b/c/d.txt")
    p = PathFs(a)
    assert p.match("d.txt") == a.match("d.txt")
    assert p.match("c/*.txt") == a.match("c/*.txt")
    assert p.match("/c/d.txt") == a.match("/c/d.txt")


def test_relative_to(tmp_path: pathlib.Path):
    a = tmp_path / "a/b/c.txt"
    p = PathFs(a)
    assert p.relative_to(str(tmp_path)) == "a/b/c.txt"
    with pytest.raises(ValueError):
        p.relative_to("/etc")


# Just wraps fsspec ----------------------------------


def test_protocol():
    p = PathFs("a.txt")
    assert p.protocol == "file"

    p = PathFs("file://a.txt")
    assert p.protocol == "file"

    p = PathFs("simplecache::file://a.txt")
    assert p.protocol == "file"

    p = PathFs("s3://a/b.txt")
    assert p.protocol == "s3"

    p = PathFs("s3a://a/b.txt")
    assert p.protocol == "s3a"


def test_open(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    p = PathFs(a)
    with p.open("w") as fo:
        fo.write("Hello")
    assert a.read_text() == "Hello"


def test_ukey(tmp_path: pathlib.Path):
    p = PathFs(tmp_path)
    fs = LocalFileSystem()
    assert fs.ukey(str(tmp_path)) == p.ukey()


def test_tail(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    p = PathFs(a)
    assert p.tail(3) == b"llo"


def test_rm_file(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    assert a.exists()
    p = PathFs(a)
    p.rm_file()
    assert not a.exists()


def test_rm_file(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    a = subdir / "a.txt"
    a.touch()
    assert a.exists()
    ap = PathFs(a)
    sp = PathFs(subdir)
    with pytest.raises(IsADirectoryError):
        sp.rm_file()
    ap.rm_file()
    assert not a.exists()
    assert sp.exists()
    with pytest.raises(IsADirectoryError):
        sp.rm_file()


def test_sign():
    p = PathFs("a.txt")
    with pytest.raises(NotImplementedError):
        p.sign()


def test_size(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    p = PathFs(a)
    assert p.size() == 5


def test_read_block(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    p = PathFs(a)
    assert p.read_block(1, 3) == b"ell"


def test_rm(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subsubdir = subdir / "subsub"
    subdir.mkdir()
    subsubdir.mkdir()
    (subsubdir / "subsubsub").mkdir()
    (subsubdir / "subsubsub/e.txt").touch()
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    assert a.exists()
    assert b.exists()
    assert c.exists()
    assert d.exists()
    # Can't remove directory
    with pytest.raises(IsADirectoryError):
        PathFs(subdir).rm()
    PathFs(c).rm()  # Can remove a single file
    assert a.exists()
    assert b.exists()
    assert not c.exists()
    assert d.exists()
    PathFs(subdir).rm(recursive=True)
    assert not subdir.exists()


def test_pipe_file(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    PathFs(a).pipe_file(b"Hello")
    assert a.read_text() == "Hello"


def test_modified(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    time.sleep(0.01)  # Need to update modification time
    p = PathFs(a)
    m1 = p.modified()
    m2 = p.modified()
    assert m1 == m2
    a.write_text("Hello")
    m3 = p.modified()
    assert m1 != m3


def test_head(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    p = PathFs(a)
    assert p.head(3) == b"Hel"


def test_info(tmp_path: pathlib.Path):
    info = PathFs(tmp_path).info()
    assert isinstance(info, dict)
    assert list(info.keys()) == ["name", "size", "type", "created", "islink", "mode", "uid", "gid", "mtime"]


def test_invalidate_cache(tmp_path: pathlib.Path):
    PathFs(tmp_path).invalidate_cache()


def test_isdir(tmp_path: pathlib.Path):
    assert PathFs(tmp_path).isdir()


def test_isfile(tmp_path: pathlib.Path):
    assert PathFs(tmp_path).isdir()
    a = tmp_path / "a.txt"
    a.touch()
    assert PathFs(a).isfile()


def test_lexists(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    p = PathFs(b)
    assert p.lexists()
    assert not p.exists()


def test_du(tmp_path: pathlib.Path):
    assert PathFs(tmp_path).du() == 0
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert PathFs(tmp_path).du() > 0


def test_exists(tmp_path: pathlib.Path):
    assert PathFs(tmp_path).exists()  # directory
    a = tmp_path / "a.txt"
    assert not PathFs(a).exists()
    a.touch()
    assert PathFs(a).exists()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert PathFs(b).exists()  # symbolic link


def test_created(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    assert isinstance(PathFs(a).created(), datetime.datetime)


def test_cat(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert PathFs(a).cat() == b"Hello"


def test_checksum(tmp_path: pathlib.Path):
    pass


def test_clear_instance_cache(tmp_path: pathlib.Path):
    pass


def test_rmdir(tmp_path: pathlib.Path):
    pass


# Others ----------------------------------------


def test_eq(tmp_path: pathlib.Path):
    pass


def test_repr(tmp_path: pathlib.Path):
    pass


def test_truediv(tmp_path: pathlib.Path):
    pass


def test_sep(tmp_path: pathlib.Path):
    pass


def test_anchor(tmp_path: pathlib.Path):
    pass


def test_parents(tmp_path: pathlib.Path):
    pass


def test_parent(tmp_path: pathlib.Path):
    pass


def test_has_parent(tmp_path: pathlib.Path):
    pass


def test_joinpath(tmp_path: pathlib.Path):
    pass


def test_with_name(tmp_path: pathlib.Path):
    pass


def test_with_suffix(tmp_path: pathlib.Path):
    pass


def test_glob(tmp_path: pathlib.Path):
    pass


def test_put(tmp_path: pathlib.Path):
    pass


def test_mv(tmp_path: pathlib.Path):
    pass


def test_ls(tmp_path: pathlib.Path):
    pass


def test_get(tmp_path: pathlib.Path):
    pass


def test_find(tmp_path: pathlib.Path):
    pass


def test_expand_path(tmp_path: pathlib.Path):
    pass


def test_path(tmp_path: pathlib.Path):
    pass


def test_fullpath(tmp_path: pathlib.Path):
    pass


def test_urlpath(tmp_path: pathlib.Path):
    pass


def test_iterdir(tmp_path: pathlib.Path):
    pass


def test_mkdir(tmp_path: pathlib.Path):
    pass


def test_read_bytes(tmp_path: pathlib.Path):
    pass


def test_read_text(tmp_path: pathlib.Path):
    pass


def test_samefile(tmp_path: pathlib.Path):
    pass


def test_touch(tmp_path: pathlib.Path):
    pass


def test_write_text(tmp_path: pathlib.Path):
    pass


def test_rglob(tmp_path: pathlib.Path):
    pass


def test_copy(tmp_path: pathlib.Path):
    pass


def test_makedirs(tmp_path: pathlib.Path):
    pass


def test_walk(tmp_path: pathlib.Path):
    pass
