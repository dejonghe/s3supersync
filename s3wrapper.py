import boto3
import sys
from logger import logging, logger, console_logger

class S3Wrapper(object):
    def __init__(self,profile,local,dest):
        session = boto3.session.Session(profile_name=profile)
        self.s3 = session.resource('s3')
        self.client = session.client('s3')
        self.local = local 
        self.dest = dest
        self.bucket = self._get_bucket(dest)
        self.key = self._get_key(dest)
        self.path = '{}/{}'.format(self.bucket,self.key)
        self._check_or_create_bucket()

    def _get_bucket(self, dest):
        bucket = dest.split('/')[2]
        return dest.split('/')[2]

    def _get_key(self, dest):
        keys = dest.split('/')[3:]
        return '/'.join(keys)

    def _check_or_create_bucket(self):
        found = False
        resp = self.client.list_buckets()
        for bucket in resp['Buckets']:
            if bucket['Name'] == self.bucket:
                found = True
        if not found:
            logger.critical('Bucket {} does not exist or is not owned by you.'.format(self.bucket))
            exit(1)

    def get_object_metadata(self):
        try: 
            resp = self.client.head_object(
                Bucket=self.bucket,
                Key=self.key,
                PartNumber=1
            )
            return resp
        except Exception as e:
            logger.warn(e)
            return None

    def get_object_chunk_size(self):
        try: 
            resp = self.client.head_object(
                Bucket=self.bucket,
                Key=self.key,
                PartNumber=1
            )
            return int(resp['ResponseMetadata']['HTTPHeaders']['content-length'])
        except Exception as e:
            logger.warn(e)
            return None

    def get_part_head(self,bucket,key,version,part):
        resp = self.client.head_object(
            Bucket=bucket,
            Key=key,
            VersionId=version,
            PartNumber=int(part)
        )
        logger.debug('Part Head: {}'.format(resp))
        return resp

    def list_multipart_uploads(self):
        resp = self.client.list_multipart_uploads(
            Bucket=self.bucket
        )
        logger.debug('List of Multipart Uploads: {}'.format(resp))
        return resp

    def list_parts(self):
        resp = self.client.list_parts(
            Bucket=self.bucket,
            Key=self.key
        )
        logger.debug('List of Parts: {}'.format(resp))
        return resp

    def start_multipart_upload(self):
        resp = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.key
        )
        logger.debug('Start Multipart Upload: {}'.format(resp))
        return resp['UploadId']
      

    def complete_multipart_upload(self,upload_id,parts):
        ordered_parts = [None]*len(parts)
        parts = list(
            map(
                lambda x: 
                    {
                        'ETag':x['ETag'],
                        'PartNumber':x['PartNumber']
                    },
                    parts
            )
        )
        for part in parts:
            ordered_parts[part['PartNumber']-1] = part
        logger.debug('Parts: {}'.format(ordered_parts))
        logger.debug('Parts Type: {}'.format(type(parts)))
        resp = self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            MultipartUpload={
               'Parts': ordered_parts
            },
            UploadId=upload_id
        )
        logger.debug('Complete Multipart Upload: {}'.format(resp))
        return resp

    def upload_part(self,chunk,part_number,upload_id):
        resp = self.client.upload_part(
           Body=chunk,
           Bucket=self.bucket,
           ContentLength=sys.getsizeof(chunk),
           Key=self.key,
           PartNumber=part_number,
           UploadId=upload_id
        )
        logger.debug('Upload Part: {}'.format(resp))
        return resp

    def copy_part(self,
            source_bucket,
            source_key,
            source_version,
            source_part_number,
            source_range,
            part_number,
            upload_id):
        resp = self.client.upload_part_copy(
           Bucket=self.bucket,
           CopySource={
               'Bucket': source_bucket,
               'Key': source_key,
               'VersionId': source_version
           },
           CopySourceRange='bytes={}'.format(source_range),
           Key=self.key,
           PartNumber=part_number,
           UploadId=upload_id
        )
        logger.debug('Upload Part: {}'.format(resp))
        return resp


