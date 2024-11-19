# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for file utils.
"""
import os
import tempfile

from django.test import TestCase

from core.utils.file import get_directory_size, format_size


class TestFileUtilities(TestCase):
    """Test File utilities."""

    def setUp(self):
        """Set test class."""
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_dir_path = self.test_dir.name

        # Create files and subdirectories for testing
        self.file1 = os.path.join(self.test_dir_path, "file1.txt")
        self.file2 = os.path.join(self.test_dir_path, "file2.txt")
        self.sub_dir = os.path.join(self.test_dir_path, "subdir")
        os.mkdir(self.sub_dir)

        self.sub_file = os.path.join(self.sub_dir, "subfile.txt")

        # Write data to files
        with open(self.file1, "wb") as f:
            f.write(b"a" * 1024)  # 1 KB
        with open(self.file2, "wb") as f:
            f.write(b"b" * 2048)  # 2 KB
        with open(self.sub_file, "wb") as f:
            f.write(b"c" * 4096)  # 4 KB

    def tearDown(self):
        """Clean up temporary directory."""
        self.test_dir.cleanup()

    def test_get_directory_size(self):
        """Test get directory size."""
        # Expected total size: 1 KB + 2 KB + 4 KB = 7 KB
        expected_size = 1024 + 2048 + 4096
        calculated_size = get_directory_size(self.test_dir_path)
        self.assertEqual(calculated_size, expected_size)

    def test_format_size(self):
        """Test format_size function."""
        test_cases = [
            (512, "512.00 B"),
            (1024, "1.00 KB"),
            (1536, "1.50 KB"),
            (1048576, "1.00 MB"),
            (1073741824, "1.00 GB"),
            (1099511627776, "1.00 TB"),
            (1125899906842624, "1.00 PB"),
        ]

        for size, expected in test_cases:
            with self.subTest(size=size):
                self.assertEqual(format_size(size), expected)
