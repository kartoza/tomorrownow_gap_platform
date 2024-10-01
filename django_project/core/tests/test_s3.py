# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit test for S3 utils.
"""
import os

from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.test import TestCase

from core.utils.s3 import zip_folder_in_s3, remove_s3_folder


class TestS3Utilities(TestCase):
    """Test S3 utilities."""

    def test_zip_folder_in_s3(self):
        """Test zip folder in S3."""
        folder = 'test_folder'
        remove_s3_folder(default_storage, folder)
        default_storage.save(
            os.path.join(folder, 'test'), ContentFile(b"new content")
        )
        default_storage.save(
            os.path.join(folder, 'test_2'), ContentFile(b"new content")
        )
        zip_folder_in_s3(
            default_storage, folder, 'test_folder.zip'
        )
        self.assertTrue(
            default_storage.exists('test_folder.zip')
        )
        self.assertFalse(
            default_storage.exists(folder)
        )
