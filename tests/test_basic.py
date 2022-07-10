import pathlib

import pytest

from pathlibfs import Path


def test_protocol():
    p = Path("a.txt")
    assert p.protocol == "file"

    p = Path("file://a.txt")
    assert p.protocol == "file"

    p = Path("simplecache::file://a.txt")
    assert p.protocol == "file"

    p = Path("s3://a/b.txt")
    assert p.protocol == "s3"

    p = Path("s3a://a/b.txt")
    assert p.protocol == "s3a"


def test_eq(tmp_path: pathlib.Path):
    assert Path("a.txt") == Path("a.txt")
    assert Path("a/b.txt") != Path("a/c.txt")
    assert Path("file://a/b.txt") != Path("s3://a/c.txt")
    # ignore chain
    assert Path("simplecache::file://b.txt") == Path("file://b.txt")
    # error
    with pytest.raises(ValueError):
        assert Path("b.txt") == "b.txt"


def test_instance():
    p = Path("a.txt")
    p2 = Path(p)
    assert p == p2
    assert id(p) != id(p2)


def test_samefile():
    p = Path("a.txt")
    assert p.samefile("a.txt")
    assert not p.samefile("a.jpg")
    assert p.samefile(Path("a.txt"))
