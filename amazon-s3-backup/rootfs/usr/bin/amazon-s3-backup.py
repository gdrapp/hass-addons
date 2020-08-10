import sys
import time
from pathlib import Path
import logging
import os
from typing import List
import datetime

import boto3

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

logging.basicConfig()
logger = logging.getLogger(__name__)

DEFAULT_S3_STORAGE_CLASS = "standard"

class S3Bucket:
    def __init__(self, bucket_name: str, bucket_region: str, storage_class: str):
        """Class representing an S3 bucket

        Args:
            bucket_name (str): Name of S3 bucket
            bucket_region (str): AWS region in which the bucket lives
            storage_class (str): S3 storage class to use for uploads
        """
        self.bucket_name = bucket_name
        self.storage_class = storage_class

        aws_config = {
            "region_name": bucket_region
        }
        logger.debug("Creating S3 client")
        self.s3_client = boto3.client("s3", **aws_config)

    def list_bucket(self) -> List:
        """List objects in the S3 bucket

        Raises:
            Exception: Thrown if bucket is not found or inaccessible

        Returns:
            List: List of objects {"name": str, "size": int, "last_modified": datetime.datetime}
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        except self.s3_client.exceptions.NoSuchBucket as err:
            logger.exception(f"Error listing objects in S3 bucket: {err}")
            raise Exception
        else:
            if response.get("IsTruncated") == True:
                logger.warning(
                    "Uh oh, received truncated results from S3 list object function and we haven't coded for that scenario. Somebody submit a pull request!")

            return [{"name": obj.get("Key"), "size": obj.get("Size"), "last_modified": obj.get("LastModified")} for obj in (response.get("Contents") or [])]

    def upload_file(self, file: str):
        """Upload file to S3 bucket

        Args:
            file (str): Full path of file to upload
        """
        file_name = Path(file).name
        extra_args = {"StorageClass": self.storage_class}

        try:
            logger.info(f"Uploading file [{file}] to S3")
            self.s3_client.upload_file(Filename=file,
                                       Bucket=self.bucket_name,
                                       Key=file_name,
                                       ExtraArgs=extra_args)
            logger.info(
                f"Uploaded file [{file_name}] to S3 bucket [{self.bucket_name}] using storage class [{self.storage_class}]")
        except boto3.exceptions.S3UploadFailedError as err:
            logger.exception(f"S3 upload error: {err}")


class BackupEventHandler(RegexMatchingEventHandler):
    BACKUP_REGEX = [r".+\.tar$"]
    logger = logging.getLogger(__name__)

    def __init__(self, s3_bucket: S3Bucket):
        """Handle new files in the HASS backup directory

        Args:
            s3_bucket (S3Bucket): S3 bucket to upload files to
        """
        super().__init__(self.BACKUP_REGEX)
        self.s3_bucket = s3_bucket

    def on_created(self, event):
        self.process(event)

    def process(self, event):
        """Process a new file

        Args:
            event:
        """
        logger.info(f"Processing new file {event.src_path}")

        file_size = -1
        while file_size != os.path.getsize(event.src_path):
            file_size = os.path.getsize(event.src_path)
            time.sleep(1)

        file_name = Path(event.src_path).name
        self.s3_bucket.upload_file(event.src_path)


class FileWatcher:
    logger = logging.getLogger(__name__)

    def __init__(self, monitor_path: str, s3_bucket: S3Bucket):
        """Watch for new files in the backup directory

        Args:
            monitor_path (str): Path to monitor for new fiels
            s3_bucket (S3Bucket): S3 bucket to upload files to
        """
        self.monitor_path = monitor_path
        self.event_handler = BackupEventHandler(s3_bucket)
        self.event_observer = Observer()

    def run(self):
        self.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def start(self):
        self.schedule()
        self.event_observer.start()

    def stop(self):
        self.event_observer.stop()
        self.event_observer.join()

    def schedule(self):
        self.event_observer.schedule(
            self.event_handler,
            self.monitor_path,
            recursive=True
        )


def set_log_level(hass_log_level: str):
    """Set this script's log level based on the level set in the HASS config for this addon

    Args:
        hass_log_level (str): A log level (1-8)
    """
    if hass_log_level:
        hass_log_level = hass_log_level.strip()

    level_map = {
        "8": logging.NOTSET,
        "7": logging.DEBUG,
        "6": logging.DEBUG,
        "5": logging.INFO,
        "4": logging.INFO,
        "3": logging.WARNING,
        "2": logging.ERROR,
        "1": logging.CRITICAL
    }
    logger.setLevel(level_map.get(hass_log_level, logging.NOTSET))


if __name__ == "__main__":
    set_log_level(os.environ.get("LOG_LEVEL"))

    if (len(sys.argv) != 6):
        logger.critical(
            f"Incorrect number of command line arguments provided: {sys.argv}")
    else:
        (_, monitor_path, bucket_name, bucket_region,
         storage_class, upload_missing_files) = sys.argv

        upload_missing_files = True if upload_missing_files.lower() == "true" else False
        s3_bucket = S3Bucket(bucket_name, bucket_region, storage_class)
        obj_monitor_path = Path(monitor_path)

        if obj_monitor_path.exists():
            bucket_contents = []
            try:
                bucket_contents = s3_bucket.list_bucket()
            except Exception:
                sys.exit(1)

            local_files = [
                x.name for x in obj_monitor_path.iterdir() if x.is_file()]

            for file in local_files:
                local_file = Path(obj_monitor_path, file)
                local_file_size = local_file.stat().st_size
                found = [f for f in bucket_contents if f.get("name") == file]
                if found:
                    if local_file_size == found[0]["size"]:
                        logger.debug(
                            f"Local file [{local_file}] found in S3 with matching size of {local_file_size}")
                    else:
                        logger.warning(
                            f"Local file [{local_file}] does not match the file in S3")
                        s3_bucket.upload_file(str(local_file))
                else:
                    logger.warning(
                        f"Local file [{local_file}] not found in S3")
                    if upload_missing_files:
                        s3_bucket.upload_file(str(local_file))

            logger.info(f"Monitoring path [{monitor_path}] for new snapshots")
            FileWatcher(monitor_path, s3_bucket).run()
        else:
            logger.critical(f"monitor_path [{monitor_path}] does not exist")
