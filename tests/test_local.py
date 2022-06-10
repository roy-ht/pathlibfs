from pathlibfs import PathFs


def text_exists(tmp_path):
    """Test exists method"""
    a = tmp_path.joinpath("a.txt")
    a.touch()
    PathFs(str(a))
    assert a.exists()
