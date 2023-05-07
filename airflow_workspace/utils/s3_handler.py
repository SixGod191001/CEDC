# -*- coding: utf-8 -*-
"""
@Author : YANG YANG
@Date : 2023/4/16 1:26
"""
import json
import random
import uuid
import boto3
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowFailException  # make the task failed without retry
from airflow.exceptions import AirflowException  # failed with retry
from airflow_workspace.utils.logger_handler import logger

logger = logger()


class S3Handler:
    def __init__(self, s3_object):
        """
        :param s3_object: A Boto3 Object resource. This is a high-level resource in Boto3
                          that wraps object actions in a class-like structure.
        """
        self.object = s3_object
        self.key = self.object.key

    def put(self, data):
        """
        Upload data to the object.
        :param data: The data to upload. This can either be bytes or a string. When this
                     argument is a string, it is interpreted as a file name, which is
                     opened in read bytes mode.
        """
        put_data = data
        if isinstance(data, str):
            try:
                put_data = open(data, 'rb')
            except IOError:
                logger.exception("Expected file name or binary data, got '%s'.", data)
                raise AirflowFailException("Expected file name or binary data, got '%s'.", data)

        try:
            self.object.put(Body=put_data)
            self.object.wait_until_exists()
            logger.info(
                "Put object '%s' to bucket '%s'.", self.object.key,
                self.object.bucket_name)
        except ClientError:
            logger.exception(
                "Couldn't put object '%s' to bucket '%s'.", self.object.key,
                self.object.bucket_name)
            raise AirflowFailException("Couldn't put object '%s' to bucket '%s'.", self.object.key,
                                       self.object.bucket_name)
        finally:
            if getattr(put_data, 'close', None):
                put_data.close()

    def get(self):
        """
        Gets the object.
        :return: The object data in bytes.
        """
        try:
            body = self.object.get()['Body'].read()
            logger.info(
                "Got object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
        except ClientError:
            logger.exception(
                "Couldn't get object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
            raise AirflowFailException("Couldn't get object '%s' from bucket '%s'.",
                                       self.object.key, self.object.bucket_name)
        else:
            return body

    @staticmethod
    def list(bucket, prefix=None):
        """
        Lists the objects in a bucket, optionally filtered by a prefix.
        :param bucket: The bucket to query. This is a Boto3 Bucket resource.
        :param prefix: When specified, only objects that start with this prefix are listed.
        :return: The list of objects.
        """
        try:
            if not prefix:
                objects = list(bucket.objects.all())
            else:
                objects = list(bucket.objects.filter(Prefix=prefix))
            logger.info("Got objects %s from bucket '%s'",
                        [o.key for o in objects], bucket.name)
        except ClientError:
            logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
            raise AirflowFailException("Couldn't get objects for bucket '%s'.", bucket.name)
        else:
            return objects

    def copy(self, dest_object):
        """
        Copies the object to another bucket.
        :param dest_object: The destination object initialized with a bucket and key.
                            This is a Boto3 Object resource.
        """
        try:
            dest_object.copy_from(CopySource={
                'Bucket': self.object.bucket_name,
                'Key': self.object.key
            })
            dest_object.wait_until_exists()
            logger.info(
                "Copied object from %s:%s to %s:%s.",
                self.object.bucket_name, self.object.key,
                dest_object.bucket_name, dest_object.key)
        except ClientError:
            logger.exception(
                "Couldn't copy object from %s/%s to %s/%s.",
                self.object.bucket_name, self.object.key,
                dest_object.bucket_name, dest_object.key)
            raise AirflowFailException("Couldn't copy object from %s/%s to %s/%s.",
                                       self.object.bucket_name, self.object.key,
                                       dest_object.bucket_name, dest_object.key)

    def delete(self):
        """
        Deletes the object.
        """
        try:
            self.object.delete()
            self.object.wait_until_not_exists()
            logger.info(
                "Deleted object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
        except ClientError:
            logger.exception(
                "Couldn't delete object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
            raise AirflowFailException("Couldn't delete object '%s' from bucket '%s'.",
                                       self.object.key, self.object.bucket_name)

    @staticmethod
    def delete_objects(bucket, object_keys):
        """
        Removes a list of objects from a bucket.
        This operation is done as a batch in a single request.
        :param bucket: The bucket that contains the objects. This is a Boto3 Bucket
                       resource.
        :param object_keys: The list of keys that identify the objects to remove.
        :return: The response that contains data about which objects were deleted
                 and any that could not be deleted.
        """
        try:
            response = bucket.delete_objects(Delete={
                'Objects': [{
                    'Key': key
                } for key in object_keys]
            })
            if 'Deleted' in response:
                logger.info(
                    "Deleted objects '%s' from bucket '%s'.",
                    [del_obj['Key'] for del_obj in response['Deleted']], bucket.name)
            if 'Errors' in response:
                logger.warning(
                    "Could not delete objects '%s' from bucket '%s'.", [
                        f"{del_obj['Key']}: {del_obj['Code']}"
                        for del_obj in response['Errors']],
                    bucket.name)
        except ClientError:
            logger.exception("Couldn't delete any objects from bucket %s.", bucket.name)
            raise AirflowFailException("Couldn't delete any objects from bucket %s.", bucket.name)
        else:
            return response

    @staticmethod
    def empty_bucket(bucket):
        """
        Remove all objects from a bucket.
        :param bucket: The bucket to empty. This is a Boto3 Bucket resource.
        """
        try:
            bucket.objects.delete()
            logger.info("Emptied bucket '%s'.", bucket.name)
        except ClientError:
            logger.exception("Couldn't empty bucket '%s'.", bucket.name)
            raise AirflowFailException("Couldn't empty bucket '%s'.", bucket.name)


def s3handler_usage_demo():
    print('-' * 88)
    print("Welcome to the Amazon S3 object demo!")
    print('-' * 88)

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(f'doc-example-bucket-{uuid.uuid4()}')
    try:
        bucket.create(
            CreateBucketConfiguration={
                'LocationConstraint': s3_resource.meta.client.meta.region_name})
    except ClientError as err:
        print(
            f"Couldn't create a bucket for the demo. Here's why: "
            f"{err.response['Error']['Message']}.")

    object_key = 'doc-example-object'
    obj_wrapper = S3Handler(bucket.Object(object_key))
    obj_wrapper.put(__file__)
    print(f"Put file object with key {object_key} in bucket {bucket.name}.")

    with open(__file__) as file:
        lines = file.readlines()

    line_wrappers = []
    for _ in range(10):
        line = random.randint(0, len(lines))
        line_wrapper = S3Handler(bucket.Object(f'line-{line}'))
        line_wrapper.put(bytes(lines[line], 'utf-8'))
        line_wrappers.append(line_wrapper)
    print(f"Put 10 random lines from this script as objects.")

    listed_lines = S3Handler.list(bucket, 'line-')
    print(f"Their keys are: {', '.join(l.key for l in listed_lines)}")

    line = line_wrappers.pop()
    line_body = line.get()
    print(f"Got object with key {line.key} and body {line_body}.")
    line.delete()
    print(f"Deleted object with key {line.key}.")

    copied_obj = bucket.Object(line_wrappers[0].key + '-copy')
    line_wrappers[0].copy(copied_obj)
    print(f"Made a copy of object {line_wrappers[0].key}, named {copied_obj.key}.")

    S3Handler.empty_bucket(bucket)
    print(f"Emptied bucket {bucket.name} in preparation for deleting it.")

    bucket.delete()
    print(f"Deleted bucket {bucket.name}.")
    print("Thanks for watching!")
    print('-' * 88)


if __name__ == '__main__':
    s3handler_usage_demo()
