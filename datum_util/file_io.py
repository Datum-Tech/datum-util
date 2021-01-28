"""File IO."""
import logging
import os
from typing import List

from fsspec.core import get_fs_token_paths

from .typing import PathType

logger = logging.getLogger(__name__)


def get_dir_list(path: PathType) -> List[str]:
    """Get directory list using fsspec."""
    fs, fs_token, paths, protocol = get_fs_info(path)
    if fs.exists(path):
        return [f"{protocol}://{_}" for _ in fs.ls(path)]
    return []


def glob_path(path: PathType, glob: str) -> List[str]:
    """Get globbed paths with protocol."""
    fs, fs_token, paths, protocol = get_fs_info(path)
    if fs.exists(path):
        return [f"{protocol}://{_}" for _ in fs.glob(f"{path}/{glob}")]
    return []


def exists(path: PathType) -> bool:
    """Use fsspec to check if file exists."""
    fs, fs_token, paths, protocol = get_fs_info(path)
    return fs.exists(path)


def get_fs_info(path: PathType):
    """Get fsspec filesystem from path.

    Usage
    -----
    >>> fs, fs_token, paths, protocol = get_fs_info(path)

    Returns
    -------
    fs
        Fsspec filesystem object.
    fs_token
        Filesystem token
    paths
        Expanded list of paths matching path.
    protocol
        Protocol of fs object.

    """
    storage_options = {}
    if os.getenv("COMPUTE_ENV") == "GCP":
        storage_options = {"token": "cloud"}
    fs, fs_token, paths = get_fs_token_paths(path,
                                             storage_options=storage_options)
    protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    return fs, fs_token, paths, protocol
