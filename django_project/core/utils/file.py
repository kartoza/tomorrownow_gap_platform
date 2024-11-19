# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Utilities for file.
"""
import os


def get_directory_size(path):
    """Get size of directory.

    :param path: directory path
    :type path: string
    :return: size in bytes
    :rtype: int
    """
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            # Add file size, skipping broken symbolic links
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
    return total_size


def format_size(size_in_bytes):
    """Format size to human readable.

    :param size_in_bytes: size
    :type size_in_bytes: int
    :return: human readable size
    :rtype: str
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.2f} PB"
