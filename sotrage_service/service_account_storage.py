from typing import Optional, Dict, Any, List
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions
from datetime import datetime, timedelta
import logging
import os
from storage_config import STORAGE_CONFIG

import json
from pathlib import Path
import uuid

logger = logging.getLogger(__name__)

class ServiceAccountStorage:
    """Service for managing Google Cloud Storage operations using service account"""
    
    def __init__(self):
        """Initialize with service account credentials from dastavez-sa.json"""
        self._logger = logging.getLogger(__name__)
        self.config = STORAGE_CONFIG
        try:
            # Get the path to the service account file
            sa_path = Path(__file__).parent.parent.parent.parent.resolve() / 'common' / 'dastavez-sa.json'
            with open(sa_path) as f:
                service_account_info = json.load(f)
                
            self.project_id = service_account_info['project_id']
            self.credentials = service_account.Credentials.from_service_account_info(
                service_account_info
            )
            self.client = storage.Client(
                project=self.project_id,
                credentials=self.credentials
            )
            
        except Exception as e:
            self._logger.error(f"Failed to initialize storage service: {str(e)}")
            raise

    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """Get a bucket by name"""
        try:
            bucket = self.client.get_bucket(bucket_name)
            return bucket
        except Exception as e:
            self._logger.error(f"Failed to get bucket {bucket_name}: {str(e)}")
            raise

    def create_bucket(self, bucket_name: str, location: str = "US") -> storage.Bucket:
        """Create a new bucket"""
        try:
            bucket = self.client.create_bucket(bucket_name, location=location)
            self._logger.info(f"Created bucket {bucket_name}")
            return bucket
        except Exception as e:
            self._logger.error(f"Failed to create bucket {bucket_name}: {str(e)}")
            raise

    def get_or_create_bucket(self, bucket_name: str):
        """Get an existing bucket or create a new one with configured settings"""
        try:
            # Try to get existing bucket first
            bucket = self.client.bucket(bucket_name)
            if bucket.exists():
                self._logger.info(f"Using existing bucket: {bucket_name}")
                return bucket
            
            # Create new bucket with basic settings first
            bucket = self.client.bucket(bucket_name)
            bucket.create(location=self.config['location'])
            
            # Then set additional properties
            bucket.storage_class = self.config['default_storage_class']
            bucket.labels = self.config['labels']
            bucket.patch()  # Save storage class and labels
            
            # Set lifecycle rules
            bucket.lifecycle_rules = self.config['lifecycle_rules']
            bucket.patch()  # Save lifecycle rules
            
            self._logger.info(
                f"Created new bucket: {bucket_name} in {self.config['location']} "
                f"with storage class {self.config['default_storage_class']}"
            )
            return bucket
            
        except Exception as e:
            self._logger.error(f"Error with bucket {bucket_name}: {str(e)}")
            raise e

    def upload_file(self, bucket_name: str, file_path: str):
        """Upload a file to the specified bucket"""
        try:
            # Get or create bucket
            bucket = self.get_or_create_bucket(bucket_name)
            
            # Upload file
            blob = bucket.blob(file_path.split('/')[-1])  # Use filename as blob name
            blob.upload_from_filename(file_path)
            self._logger.info(f"Successfully uploaded {file_path} to {bucket_name}")
            return blob.name
            
        except Exception as e:
            self._logger.error(f"Failed to upload file {file_path}: {str(e)}")
            return None

    def download_file(self, bucket_name: str, source_blob_name: str, destination_file_path: str) -> None:
        """Download a file from the bucket"""
        try:
            bucket = self.get_bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_path)
            self._logger.info(f"Downloaded {source_blob_name} to {destination_file_path}")
        except Exception as e:
            self._logger.error(f"Failed to download file {source_blob_name}: {str(e)}")
            raise

    def delete_file(self, bucket_name: str, blob_name: str) -> None:
        """Delete a file from the bucket"""
        try:
            bucket = self.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            self._logger.info(f"Deleted file {blob_name} from bucket {bucket_name}")
        except Exception as e:
            self._logger.error(f"Failed to delete file {blob_name}: {str(e)}")
            raise

    def list_files(self, bucket_name: str):
        """List all files in the bucket"""
        try:
            bucket = self.client.bucket(bucket_name)
            return [blob.name for blob in bucket.list_blobs()]
        except Exception as e:
            self._logger.error(f"Failed to list files: {str(e)}")
            return []

    def get_signed_url(self, bucket_name: str, blob_name: str):
        """Generate signed URL for a blob"""
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=15),
                method="GET"
            )
            return url
        except Exception as e:
            self._logger.error(f"Failed to generate signed URL: {str(e)}")
            return None

