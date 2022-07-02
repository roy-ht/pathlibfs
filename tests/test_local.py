"""Test Local filesystem

Check wrapping methods as simple as possible
"""

import pathlib

from pathlibfs import PathFs

# Just wraps pathlib if local filesystem ---------------------------


def test_drive(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.drive == p.drive


def test_root(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.root == p.root


def test_as_posix(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.as_posix() == p.as_posix()


def test_chmod(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    p = PathFs(str(a))
    p.chmod(0o775)
    assert a.stat().st_mode == 0o100775


def test_group(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.group() == p.group()


def test_is_mount(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_mount() == p.is_mount()


def test_is_symlink(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_symlink() == p.is_symlink()
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert PathFs(str(b)).is_symlink()


def test_is_socket(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_socket() == p.is_socket()


def test_is_fifo(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_fifo() == p.is_fifo()


def test_is_block_device(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_block_device() == p.is_block_device()


def test_is_char_device(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_char_device() == p.is_char_device()


def test_owner(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.owner() == p.owner()


def test_is_absolute(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_absolute() == p.is_absolute()
    pp = PathFs("a.txt")
    assert pp.is_absolute()  # local filesystem always absolute


def test_is_reserved(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.is_reserved() == p.is_reserved()


def test_resolve(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    p = PathFs(str(b))
    pp = p.resolve()
    assert pp.name == "a.txt"


def test_symlink_to(tmp_path: pathlib.Path):
    a = tmp_path / "a.txt"
    a.touch()
    b = tmp_path / "b.txt"
    b.symlink_to(a)
    assert b.is_symlink()


def test_parts(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.parts == p.parts


def test_name(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.name == p.name


def test_suffix(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = PathFs(str(a))
    assert p.suffix == ".gz"


def test_suffixes(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = PathFs(str(a))
    assert p.suffixes == [".tar", ".gz"]


def test_stem(tmp_path: pathlib.Path):
    a = tmp_path / "a.tar.gz"
    p = PathFs(str(a))
    assert p.stem == "a.tar"


def test_as_uri(tmp_path: pathlib.Path):
    p = PathFs(str(tmp_path))
    assert tmp_path.as_uri() == p.as_uri()


def test_match(tmp_path: pathlib.Path):
    pass


def test_relative_to(tmp_path: pathlib.Path):
    pass


# Just wraps fsspec ----------------------------------


def test_protocol():
    pass


def test_open():
    pass


def test_ukey():
    pass


def test_tail():
    pass


def test_rm_file():
    pass


def test_sign():
    pass


def test_size():
    pass


def test_read_block():
    pass


def test_rm():
    pass


def test_pipe_file():
    pass


def test_modified():
    pass


def test_head():
    pass


def test_info():
    pass


def test_invalidate_cache():
    pass


def test_isdir():
    pass


def test_isfile():
    pass


def test_lexists():
    pass


def test_du():
    pass


def test_exists():
    pass


def test_created():
    pass


def test_cat():
    pass


def test_checksum():
    pass


def test_clear_instance_cache():
    pass


def test_rmdir():
    pass


# Others ----------------------------------------


def test_eq():
    pass


def test_repr():
    pass


def test_truediv():
    pass


def test_sep():
    pass


def test_anchor():
    pass


def test_parents():
    pass


def test_parent():
    pass


def test_has_parent():
    pass


def test_joinpath():
    pass


def test_with_name():
    pass


def test_with_suffix():
    pass


def test_glob():
    pass


def test_put():
    pass


def test_mv():
    pass


def test_ls():
    pass


def test_get():
    pass


def test_find():
    pass


def test_expand_path():
    pass


def test_path():
    pass


def test_fullpath():
    pass


def test_urlpath():
    pass


def test_iterdir():
    pass


def test_mkdir():
    pass


def test_read_bytes():
    pass


def test_read_text():
    pass


def test_samefile():
    pass


def test_touch():
    pass


def test_write_text():
    pass


def test_rglob():
    pass


def test_copy():
    pass


def test_makedirs():
    pass


def test_walk():
    pass
