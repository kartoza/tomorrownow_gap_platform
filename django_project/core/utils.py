import io
import zipfile

from django.core.files.base import ContentFile
from storages.backends.s3boto3 import S3Boto3Storage


def zip_folder_in_s3(
        s3_storage: S3Boto3Storage, folder_path: str, zip_file_name: str
):
    """Zip folder contents into a zip file on S3."""
    zip_buffer = io.BytesIO()

    # Create buffer zip file
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        # Get file list
        files_in_folder = s3_storage.bucket.objects.filter(
            Prefix=folder_path
        )

        for s3_file in files_in_folder:
            file_name = s3_file.key.split('/')[-1]
            if not file_name:
                continue

            # Read the file and add to zip file
            file_content = s3_file.get()['Body'].read()
            zip_file.writestr(file_name, file_content)

    # Save it to S3
    zip_buffer.seek(0)
    s3_storage.save(zip_file_name, ContentFile(zip_buffer.read()))


def remove_s3_folder(s3_storage: S3Boto3Storage, folder_path: str):
    """Remove folder from S3 storage."""
    if not folder_path.endswith('/'):
        folder_path += '/'

    # Get all file in the folder and remove one by one
    bucket = s3_storage.bucket
    objects_to_delete = bucket.objects.filter(Prefix=folder_path)
    for obj in objects_to_delete:
        obj.delete()
