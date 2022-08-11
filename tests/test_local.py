"""Test Local filesystem

Check wrapping methods as simple as possible
"""

import datetime
import pathlib
import time
import types

import pytest
from fsspec.implementations.local import LocalFileSystem

from pathlibfs import Path
from pathlibfs.exception import PathlibfsException


def test_fs():
    assert isinstance(Path("a.txt").fs, LocalFileSystem)


# Just wraps pathlib if local filesystem ---------------------------


def test_drive(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.drive == p.drive


def test_root(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.root == p.root


def test_as_posix(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.as_posix() == p.as_posix()


def test_chmod(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    p = Path(a)
    p.chmod(0o775)
    assert a.stat().st_mode == 0o100775


def test_group(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.group() == p.group()


def test_is_mount(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_mount() == p.is_mount()


def test_is_symlink(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_symlink() == p.is_symlink()
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert Path(b).is_symlink()


def test_is_socket(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_socket() == p.is_socket()


def test_is_fifo(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_fifo() == p.is_fifo()


def test_is_block_device(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_block_device() == p.is_block_device()


def test_is_char_device(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_char_device() == p.is_char_device()


def test_owner(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.owner() == p.owner()


def test_is_absolute(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_absolute() == p.is_absolute()
    pp = Path("a.txt")
    assert not pp.is_absolute()  # local filesystem always absolute


def test_is_reserved(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.is_reserved() == p.is_reserved()


def test_resolve(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    p = Path(b)
    pp = p.resolve()
    assert pp.name == "a.txt"


def test_symlink_to(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    Path(b).symlink_to(a)
    assert b.is_symlink()


def test_parts(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.parts == p.parts


def test_name(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert tmp_path.name == p.name
    assert Path("/a/b/").name == "b"


def test_suffix(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = Path(a)
    assert p.suffix == ".gz"
    assert Path("README").suffix == ""


def test_suffixes(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = Path(a)
    assert p.suffixes == [".tar", ".gz"]
    assert Path("README").suffixes == []


def test_stem(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = Path(a)
    assert p.stem == "a.tar"


def test_match():
    a = pathlib.Path("a/b/c/d.txt")
    p = Path(a)
    assert p.match("d.txt") == a.match("d.txt")
    assert p.match("c/*.txt") == a.match("c/*.txt")
    assert p.match("/c/d.txt") == a.match("/c/d.txt")


def test_relative_to(tmp_path: pathlib.Path):
    a = tmp_path / "a/b/c.txt"
    p = Path(a)
    assert p.relative_to(str(tmp_path)) == "a/b/c.txt"
    with pytest.raises(ValueError):
        p.relative_to("/etc")


# Just wraps fsspec ----------------------------------


def test_open(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    p = Path(a)
    with p.open("w") as fo:
        fo.write("Hello")
    assert a.read_text() == "Hello"


def test_ukey(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    fs = LocalFileSystem()
    assert fs.ukey(str(tmp_path)) == p.ukey()


def test_tail(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    p = Path(a)
    assert p.tail(3) == b"llo"


def test_rm_file(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    a = subdir / "a.txt"
    a.touch()
    assert a.exists()
    ap = Path(a)
    sp = Path(subdir)
    with pytest.raises(IsADirectoryError):
        sp.rm_file()
    ap.rm_file()
    assert not a.exists()
    assert sp.exists()
    with pytest.raises(IsADirectoryError):
        sp.rm_file()


def test_sign():
    p = Path("a.txt")
    with pytest.raises(NotImplementedError):
        p.sign()


def test_size(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    p = Path(a)
    assert p.size() == 5


def test_read_block(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    p = Path(a)
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
        Path(subdir).rm()
    Path(c).rm()  # Can remove a single file
    assert a.exists()
    assert b.exists()
    assert not c.exists()
    assert d.exists()
    Path(subdir).rm(recursive=True)
    assert not subdir.exists()


def test_pipe_file(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    Path(a).pipe_file(b"Hello")
    assert a.read_text() == "Hello"


def test_modified(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    time.sleep(0.01)  # Need to update modification time
    p = Path(a)
    m1 = p.modified()
    m2 = p.modified()
    assert m1 == m2
    a.write_text("Hello")
    m3 = p.modified()
    assert m1 != m3


def test_head(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    p = Path(a)
    assert p.head(3) == b"Hel"


def test_info(tmp_path: pathlib.Path):
    info = Path(tmp_path).info()
    assert isinstance(info, dict)
    assert list(info.keys()) == ["name", "size", "type", "created", "islink", "mode", "uid", "gid", "mtime"]


def test_invalidate_cache(tmp_path: pathlib.Path):
    Path(tmp_path).invalidate_cache()


def test_isdir(tmp_path: pathlib.Path):
    assert Path(tmp_path).isdir()


def test_isfile(tmp_path: pathlib.Path):
    assert Path(tmp_path).isdir()
    a = tmp_path / "a.txt"
    a.touch()
    assert Path(a).isfile()


def test_lexists(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    p = Path(b)
    assert p.lexists()
    assert not p.exists()


def test_du(tmp_path: pathlib.Path):
    assert Path(tmp_path).du() == 0
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert Path(tmp_path).du() > 0


def test_exists(tmp_path: pathlib.Path):
    assert Path(tmp_path).exists()  # directory
    a = tmp_path / "a.txt"
    assert not Path(a).exists()
    a.touch()
    assert Path(a).exists()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert Path(b).exists()  # symbolic link


def test_created(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    assert isinstance(Path(a).created(), datetime.datetime)


def test_cat(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert Path(a).cat() == b"Hello"


def test_checksum(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert isinstance(Path(a).checksum(), int)


def test_clear_instance_cache(tmp_path: pathlib.Path):
    Path("a.txt").clear_instance_cache()


def test_rmdir(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    a = subdir / "a.txt"
    a.touch()
    p = Path(subdir)
    # Only accept an empty directory
    with pytest.raises(OSError):
        p.rmdir()
    Path(a).rm_file()
    p.rmdir()


# Others ----------------------------------------


def test_str(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert not str(p).startswith("file:")
    assert p.fullpath.startswith("file:")


def test_repr(tmp_path: pathlib.Path):
    fullpath_str = tmp_path.absolute().as_uri()
    assert repr(Path(tmp_path)) == f"Path({fullpath_str})"


def test_truediv(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    p = Path(tmp_path) / "a.txt"
    assert Path(a) == p


def test_sep(tmp_path: pathlib.Path):
    p = Path(".")
    assert p.sep == pathlib.PurePath(".")._flavour.sep


def test_anchor(tmp_path: pathlib.Path):
    assert tmp_path.anchor == Path(tmp_path).anchor


def test_parents(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert [str(x) for x in tmp_path.parents] == [x.path for x in p.parents]
    p = Path("a/b/c")
    assert [x.path for x in p.parents] == ["a/b", "a", "."]


def test_parent(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert str(tmp_path.parent) == p.parent.path
    p = Path("/")
    assert p.parent == p
    p = Path("")
    assert p.parent == p


def test_has_parent(tmp_path: pathlib.Path):
    p = Path(tmp_path)
    assert p.has_parent == bool(len(p.parents))
    p = Path("/")
    assert not p.has_parent


def test_joinpath(tmp_path: pathlib.Path):
    a = tmp_path.joinpath("sub", "a.txt")
    p = Path(tmp_path).joinpath("sub", "a.txt")
    assert str(a) == p.path
    p2 = Path(tmp_path).joinpath("sub").joinpath("a.txt")
    assert p == p2
    assert Path("a.txt").joinpath("/b") == Path("/b")


def test_with_name(tmp_path: pathlib.Path):
    a = Path(tmp_path) / "a.txt"
    b = a.with_name("b.txt")
    assert b == Path(tmp_path).joinpath("b.txt")


def test_with_suffix(tmp_path: pathlib.Path):
    a = Path(tmp_path) / "a.txt"
    b = a.with_suffix(".tar.gz")
    assert b == Path(tmp_path).joinpath("a.tar.gz")
    assert b.with_suffix("") == Path(tmp_path).joinpath("a.tar")


def test_glob(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subsubdir = subdir / "subsub"
    subdir.mkdir()
    subsubdir.mkdir()
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    p = Path(tmp_path)
    assert len(p.glob("**/d.txt")) == 1
    assert len(p.glob("**/*.txt")) == 4
    assert len(p.glob("sub/*.txt")) == 3
    assert len(p.glob("**/subsub/d.txt")) == 1


def test_put(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    b = tmp_path / "b.txt"
    with pytest.raises(PathlibfsException, match="must be a remote filesystem"):
        Path(a).put(b)


def test_move(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    b = tmp_path / "b.txt"
    Path(a).move(b)
    assert b.is_file()
    assert not a.exists()


def test_ls(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    assert len(Path(tmp_path).ls()) == 1


def test_get(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    b = tmp_path / "b.txt"
    with pytest.raises(PathlibfsException, match="must be a remote filesystem"):
        Path(a).get(b)


def test_find(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert len(Path(tmp_path).find()) == 1


def test_expand_path(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert len(Path(tmp_path).expand_path()) == 1


def test_path(tmp_path: pathlib.Path):
    assert Path("file://a.txt").path == "a.txt"
    assert Path("file:///etc/a.txt").path == "/etc/a.txt"
    assert Path("/etc/a.txt").path == "/etc/a.txt"
    assert Path("a/b/c.txt").path == "a/b/c.txt"
    assert Path("/a").joinpath("b.txt").path == "/a/b.txt"
    assert Path("a").joinpath("b.txt").path == "a/b.txt"


def test_fullpath(tmp_path: pathlib.Path):
    assert Path("file://a.txt").fullpath == "file://a.txt"
    assert Path("file:///etc/a.txt").fullpath == "file:///etc/a.txt"
    assert Path("/etc/a.txt").fullpath == "file:///etc/a.txt"
    assert Path("a/b/c.txt").fullpath == "file://a/b/c.txt"
    assert Path("/a").joinpath("b.txt").fullpath == "file:///a/b.txt"
    assert Path("a").joinpath("b.txt").fullpath == "file://a/b.txt"


def test_urlpath(tmp_path: pathlib.Path):
    assert Path("simplecache::file://a.txt").urlpath == "simplecache::file://a.txt"


def test_iterdir(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    itr = Path(tmp_path).iterdir()
    assert isinstance(itr, types.GeneratorType)
    assert len(list(itr)) == 1


def test_mkdir(tmp_path: pathlib.Path):
    a = tmp_path / "a"
    Path(a).mkdir()
    assert a.is_dir()
    s = tmp_path / "b" / "c"
    with pytest.raises(FileNotFoundError):
        Path(s).mkdir()
    Path(s).mkdir(parents=True)
    assert s.is_dir()
    with pytest.raises(FileExistsError):
        Path(s).mkdir()
    Path(s).mkdir(exist_ok=True)
    t = tmp_path / "a" / "d" / "e"
    with pytest.raises(FileNotFoundError):
        Path(t).mkdir(exist_ok=True)
    Path(t).mkdir(parents=True)
    Path(t).isdir()
    with pytest.raises(FileExistsError):
        Path(t).mkdir(parents=True)
    Path(t).mkdir(parents=True, exist_ok=True)


def test_read_bytes(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert Path(a).read_bytes() == b"Hello"


def test_read_text(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    assert Path(a).read_text() == "Hello"


def test_samefile(tmp_path: pathlib.Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert Path(tmp_path / "a.txt") == Path(tmp_path / "a.txt")
    assert Path("file://a/b.txt") != Path("s3://a/b.txt")
    assert Path(tmp_path / "a.txt") == Path("a.txt")


def test_touch(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    p = Path(a)
    p.touch()
    assert p.isfile()
    with pytest.raises(FileExistsError):
        p.touch(exist_ok=False)


def test_write_text(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    Path(a).write_text("Hello")
    assert a.read_text() == "Hello"


def test_rglob(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subsubdir = subdir / "subsub"
    subdir.mkdir()
    subsubdir.mkdir()
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    assert Path(tmp_path).rglob("*.txt") == [Path(a), Path(b), Path(c), Path(d)]


def test_copy(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.write_text("Hello")
    b = tmp_path / "b.txt"
    Path(a).copy(b)
    assert b.is_file()


def test_makedirs(tmp_path: pathlib.Path):
    a = tmp_path / "a" / "b"
    Path(a).makedirs()
    assert a.is_dir()


def test_walk(tmp_path: pathlib.Path):
    subdir = tmp_path / "sub"
    subsubdir = subdir / "subsub"
    subdir.mkdir()
    subsubdir.mkdir()
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    for root, _, files in Path(tmp_path).walk():
        for name in files:
            assert root / name in (Path(a), Path(b), Path(c), Path(d))
