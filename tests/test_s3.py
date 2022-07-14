"""Test S3 filesystem

Check wrapping methods as simple as possible
"""

import importlib
import pathlib
import time
import types
import uuid

import aiobotocore.session
import pytest

import pathlibfs
from pathlibfs import Path, PathlibfsException, s3_support

# Just wraps pathlib if local filesystem ---------------------------


def _connection_option():
    return {
        "key": "pathlibfs",
        "secret": "pathlibfs",
        "use_ssl": False,
        "client_kwargs": {"endpoint_url": "http://127.0.0.1:9000"},
    }


@pytest.fixture(scope="module", autouse=True)
def setup():
    p = Path("s3://pathlibfs", **_connection_option())
    if not p.exists():
        p.mkdir()


@pytest.fixture(scope="function")
def tmp():
    t = str(uuid.uuid4())
    p = Path("s3://pathlibfs", **_connection_option())
    return p / t


def test_drive(tmp: Path):
    assert tmp.drive == ""


def test_root(tmp: Path):
    assert tmp.root == ""


def test_as_posix(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.as_posix()


def test_chmod(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.chmod(0o755)


def test_group(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.group()


def test_is_mount(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.is_mount()


def test_is_symlink(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.is_symlink()


def test_is_socket(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.is_socket()


def test_is_fifo(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.is_fifo()


def test_is_block_device(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.is_block_device()


def test_is_char_device(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.is_char_device()


def test_owner(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.owner()


def test_is_absolute(tmp: Path):
    assert tmp.is_absolute()


def test_is_reserved(tmp: Path):
    assert not tmp.is_reserved()


def test_resolve(tmp: Path):
    assert tmp.resolve() == tmp


def test_symlink_to(tmp: Path):
    with pytest.raises(PathlibfsException):
        assert tmp.symlink_to(tmp / "a.txt")


def test_parts(tmp: Path):
    assert tmp.parts[0] == "pathlibfs"
    assert len(tmp.parts) == 2
    p = Path("s3:///a/b.txt")
    assert p.parts == ("a", "b.txt")
    p = Path("s3:////a/b.txt")
    assert p.parts == ("a", "b.txt")
    p = Path("s3:///a//b.txt")
    assert p.parts == ("a", "", "b.txt")


def test_name(tmp: Path):
    a = tmp / "a.txt"
    assert a.name == "a.txt"


def test_suffix(tmp: Path):
    a = tmp / "a.tar.gz"
    assert a.suffix == ".gz"


def test_suffixes(tmp: Path):
    a = tmp / "a.tar.gz"
    assert a.suffixes == [".tar", ".gz"]


def test_stem(tmp: Path):
    a = tmp / "a.tar.gz"
    assert a.stem == "a.tar"


def test_match():
    a = Path("s3://a/b/c/d.txt")
    assert a.match("d.txt")
    assert a.match("c/*.txt")
    assert not a.match("/c/d.txt")


def test_relative_to(tmp: Path):
    a = tmp / "a/b/c.txt"
    assert a.relative_to(tmp) == "a/b/c.txt"
    with pytest.raises(PathlibfsException, match="protocol must be same"):
        assert a.relative_to("a")
    with pytest.raises(ValueError, match="does not start with"):
        assert a.relative_to(Path("s3://hoge"))


# Just wraps fsspec ----------------------------------


def test_open(tmp: Path):
    a = tmp / "a.txt"
    with a.open("w") as fo:
        fo.write("Hello")
    assert a.read_text() == "Hello"


def test_ukey(tmp: Path):
    a = tmp / "a.txt"
    b = tmp / "b.txt"
    a.touch()
    b.touch()
    # calling ukey requires file existence
    assert a.ukey() != b.ukey()


def test_tail(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.tail(3) == b"llo"


def test_rm_file(tmp: Path):
    subdir = tmp / "sub"
    a = subdir / "a.txt"
    a.touch()
    assert a.exists()
    subdir.rm_file()  # NO OP
    assert a.exists()
    a.rm_file()
    assert not a.exists()
    assert not subdir.exists()


def test_sign():
    # TODO: could test with minio??
    pass
    # p = Path("s3://a/a.txt")
    # p.sign()


def test_size(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.size() == 5


def test_read_block(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.read_block(1, 3) == b"ell"


def test_rm(tmp: Path):
    subdir = tmp / "sub"
    subsubdir = subdir / "subsub"
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    e = subsubdir / "subsubsub/e.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    e.touch()
    assert a.exists()
    assert b.exists()
    assert c.exists()
    assert d.exists()
    assert e.exists()
    # Can't remove directory
    subdir.rm()
    assert subdir.exists()
    c.rm()  # Can remove a single file
    assert a.exists()
    assert b.exists()
    assert not c.exists()
    assert d.exists()
    assert e.exists()
    subdir.rm(recursive=True)
    assert not subdir.exists()
    assert not a.exists()
    assert not b.exists()
    assert not d.exists()
    assert not e.exists()


def test_pipe_file(tmp: Path):
    a = tmp / "a.txt"
    a.pipe_file(b"Hello")
    assert a.read_text() == "Hello"


def test_modified(tmp: Path):
    a = tmp / "a.txt"
    a.touch()
    time.sleep(1.2)  # time resolution of S3 is seconds
    m1 = a.modified()
    m2 = a.modified()
    assert m1 == m2
    a.write_text("Hello")
    m3 = a.modified()
    assert m1 != m3


def test_head(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.head(3) == b"Hel"


def test_info(tmp: Path):
    a = tmp / "a.txt"
    a.touch()
    info = a.info()
    assert isinstance(info, dict)
    assert set(info.keys()) == {
        "ETag",
        "LastModified",
        "size",
        "name",
        "type",
        "StorageClass",
        "VersionId",
        "ContentType",
    }


def test_invalidate_cache(tmp: Path):
    tmp.invalidate_cache()


def test_isdir(tmp: Path):
    assert not tmp.isdir()
    tmp.joinpath("a.txt").touch()
    assert tmp.isdir()


def test_isfile(tmp: Path):
    a = tmp / "a.txt"
    a.touch()
    assert a.isfile()


def test_lexists(tmp: Path):
    tmp.lexists()


def test_du(tmp: Path):
    assert tmp.du() == 0
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert tmp.du() > 0


def test_exists(tmp: Path):
    a = tmp / "a.txt"
    assert not a.exists()
    a.touch()
    assert a.exists()


def test_created(tmp: Path):
    a = tmp / "a.txt"
    a.touch()
    with pytest.raises(NotImplementedError):
        a.created()


def test_cat(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.cat() == b"Hello"


def test_checksum(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert isinstance(a.checksum(), int)


def test_clear_instance_cache(tmp: Path):
    Path("a.txt").clear_instance_cache()


def test_rmdir(tmp: Path):
    subdir = tmp / "sub"
    a = subdir / "a.txt"
    a.touch()
    # rmdir tries to remove thebucket, so no op if bucket is not empty
    with pytest.raises(PathlibfsException):
        subdir.rmdir()


# Others ----------------------------------------


def test_repr(tmp: Path):
    fullpath_str = tmp.fullpath
    assert repr(tmp) == f"Path({fullpath_str})"


def test_truediv(tmp: Path):
    a = tmp / "a.txt"
    p = tmp.joinpath("a.txt")
    assert a == p


def test_sep(tmp: Path):
    assert tmp.sep == "/"


def test_anchor(tmp: Path):
    assert tmp.anchor == ""


def test_parents(tmp: Path):
    assert [str(x) for x in tmp.parents] == ["s3://pathlibfs"]


def test_parent(tmp: Path):
    assert str(tmp.parent) == tmp.parent.fullpath
    p = Path("s3://bucket")
    assert p.parent == p


def test_has_parent(tmp: Path):
    assert tmp.has_parent
    p = Path("s3://bucket/")
    assert not p.has_parent


def test_joinpath(tmp: Path):
    a = tmp.joinpath("sub", "a.txt")
    a2 = tmp.joinpath("sub").joinpath("a.txt")
    assert a == a2


def test_with_name(tmp: Path):
    a = tmp / "a.txt"
    b = a.with_name("b.txt")
    assert b == tmp / "b.txt"


def test_with_suffix(tmp: Path):
    a = tmp / "a.txt"
    b = a.with_suffix(".tar.gz")
    assert b == tmp.joinpath("a.tar.gz")
    assert b.with_suffix("") == tmp.joinpath("a.tar")


def test_glob(tmp: Path):
    subdir = tmp / "sub"
    subsubdir = subdir / "subsub"
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    assert len(tmp.glob("**/d.txt")) == 1
    assert len(tmp.glob("**/*.txt")) == 4
    assert len(tmp.glob("sub/*.txt")) == 3
    assert len(tmp.glob("**/subsub/d.txt")) == 1


def test_put(tmp: Path, tmp_path: pathlib.Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    b = tmp / "b.txt"
    with pytest.raises(PathlibfsException, match="must be a local filesystem"):
        a.put(b)
    lc = Path(tmp_path) / "c.txt"
    lc.write_text("Spam")
    b.put(lc)
    assert b.read_text() == "Spam"


def test_move(tmp: Path, tmp_path: pathlib.Path):
    subdir = tmp / "sub"
    subsubdir = subdir / "subsub"
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    e = subsubdir / "e.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    a.move(e)
    assert e.is_file()
    assert not a.exists()
    subdir.move(tmp_path, recursive=True)
    assert not subdir.exists()
    assert tmp_path.joinpath("b.txt").exists(), list(tmp_path.iterdir())
    assert tmp_path.joinpath("subsub/d.txt").exists(), list(tmp_path.iterdir())
    assert tmp_path.joinpath("subsub/e.txt").exists(), list(tmp_path.iterdir())


def test_ls(tmp: Path):
    a = tmp / "a.txt"
    a.touch()
    assert len(tmp.ls()) == 1


def test_get(tmp: Path, tmp_path: pathlib.Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    with pytest.raises(PathlibfsException, match="must be a local filesystem"):
        a.get(tmp / "b.txt")
    c = Path(tmp_path) / "c.txt"
    a.get(c)
    assert c.read_text() == "Hello"


def test_find(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert len(tmp.find()) == 1


def test_expand_path(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert len(tmp.expand_path()) == 1


def test_path():
    assert Path("s3://a.txt").path == "a.txt"
    assert Path("s3:///etc/a.txt").path == "etc/a.txt"
    assert Path("s3://a/b/c.txt").path == "a/b/c.txt"
    assert Path("s3://a").joinpath("b.txt").path == "a/b.txt"


def test_fullpath():
    assert Path("s3://a.txt").fullpath == "s3://a.txt"
    assert Path("s3:///etc/a.txt").fullpath == "s3://etc/a.txt"
    assert Path("s3://a/b/c.txt").fullpath == "s3://a/b/c.txt"
    assert Path("s3://a").joinpath("b.txt").fullpath == "s3://a/b.txt"


def test_urlpath():
    assert Path("simplecache::s3://a/a.txt").urlpath == "simplecache::s3://a/a.txt"


def test_iterdir(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    itr = tmp.iterdir()
    assert isinstance(itr, types.GeneratorType)
    assert len(list(itr)) == 1


def test_mkdir(tmp: Path):
    """s3fs don't create directory, but bucket"""
    a = tmp / "a"
    assert not a.is_dir()
    s = tmp / "b" / "c"
    s.mkdir()
    assert not s.is_dir()


def test_read_bytes(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.read_bytes() == b"Hello"


def test_read_text(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.read_text() == "Hello"


def test_samefile():
    assert Path("s3://hoge/fuga.txt") == Path("s3://hoge/fuga.txt")
    assert Path("hoge/fuga.txt") != Path("s3://hoge/fuga.txt")


def test_touch(tmp: Path):
    a = tmp / "a.txt"
    a.touch()
    assert a.isfile()


def test_write_text(tmp: Path):
    a = tmp / "a.txt"
    a.write_text("Hello")
    assert a.read_text() == "Hello"


def test_rglob(tmp: Path):
    subdir = tmp / "sub"
    subsubdir = subdir / "subsub"
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    assert tmp.rglob("*.txt") == [a, b, c, d]


def test_copy(tmp: Path, tmp_path: pathlib.Path):
    subdir = tmp / "sub"
    subsubdir = subdir / "subsub"
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    e = subsubdir / "e.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    a.copy(e)
    assert e.is_file()
    subdir.copy(tmp_path, recursive=True)
    assert tmp_path.joinpath("a.txt").exists(), list(tmp_path.iterdir())
    assert tmp_path.joinpath("subsub/d.txt").exists(), list(tmp_path.iterdir())
    assert tmp_path.joinpath("subsub/e.txt").exists(), list(tmp_path.iterdir())


def test_makedirs(tmp: Path):
    """s3fs don't create directory, but bucket"""
    a = tmp / "a" / "b"
    a.makedirs()  # can call
    assert not a.is_dir()  # but not exist


def test_walk(tmp: Path):
    subdir = tmp / "sub"
    subsubdir = subdir / "subsub"
    a = subdir / "a.txt"
    b = subdir / "b.txt"
    c = subdir / "c.txt"
    d = subsubdir / "d.txt"
    a.touch()
    b.touch()
    c.touch()
    d.touch()
    for root, _, files in tmp.walk():
        for name in files:
            assert root / name in (a, b, c, d)


def test_register_session_cache(tmp: Path, monkeypatch):
    monkeypatch.setenv("PATHLIBFS_S3_SESSION_CACHE", "1")
    importlib.reload(pathlibfs)
    tmp.ls()
    assert aiobotocore.session.create_credential_resolver.__name__ == "_patch"
    tmp.ls()
    aio_session = aiobotocore.session.create_credential_resolver(tmp.fs.session)
    for provider in aio_session.providers:
        if hasattr(provider, "cache"):
            assert isinstance(provider.cache, s3_support.CredentialCache)


def test_session_cache(tmp_path: pathlib.Path):
    cache = s3_support.CredentialCache(str(tmp_path))
    cache["A"] = "a"
    cache["B"] = "b"
    assert tmp_path.joinpath("A.json").exists(), list(tmp_path.iterdir())
    assert tmp_path.joinpath("B.json").exists(), list(tmp_path.iterdir())
    assert cache["A"] == "a"
    assert cache["B"] == "b"
    del cache["A"]
    assert not tmp_path.joinpath("A.json").exists(), list(tmp_path.iterdir())
    assert "A" not in cache
    assert "B" in cache
    with pytest.raises(KeyError):
        cache["A"]
