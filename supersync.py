#!/usr/bin/env python3.6
import argparse
import boto3
import hashlib
import math
import os
import sys
from optparse import OptionParser
from logger import logging, logger, console_logger

KB = 1024
MB = KB * KB
GB = MB * KB

class SuperSync(object):

    def __init__(self, local, dest, table_name, profile='default'):
        self.session = boto3.session.Session(profile_name=profile)
        self.client = self.session.client('s3')
        self.dynamo = self.session.client('dynamodb')
        self.s3 = boto3.resource('s3')
        self.local = local 
        self.dest = dest
        self.table_name = table_name
        self.bucket = self._get_bucket(dest)
        self.key = self._get_key(dest)
        self.path = '{}/{}'.format(self.bucket,self.key)
        self.local_size = os.path.getsize(local)
        logger.debug(self.local_size)
        logger.debug(self.bucket)
        logger.debug(self.key)
        self.prior_object = self._get_object_metadata()
        self.chunk_size = self._get_chunk_size()
        self.part_count = self._get_part_count()
        self.multipart = self._start_multipart_upload()
        self.upload_id = self.multipart['UploadId']
        self.parts = self._chunk_file()
        self.upload = self._complete_multipart_upload()
        self.version = self.upload['VersionId']
        self._push_dynamo_metadata()
     

    def _get_bucket(self, dest):
        return dest.split('/')[2]

    def _get_key(self, dest):
        keys = dest.split('/')[3:]
        return '/'.join(keys)

    def _get_object_metadata(self):
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

    def _get_chunk_size(self):
        if self.prior_object:
            chunk_size = int(self.prior_object['ResponseMetadata']['HTTPHeaders']['content-length'])
        else:
            chunk_size = 8 * MB
            while self.local_size / chunk_size > 10000:
                logger.debug('Part size {}MB too small'.format(str(chunk_size/MB)))
                chunk_size = chunk_size * 2
        logger.debug('Part count determined: {}MB'.format(str(chunk_size/MB)))
        return chunk_size
        

    def _get_part_count(self):
        unrounded = self.local_size / self.chunk_size
        rounded = int(math.ceil(unrounded))
        logger.debug('Number of parts determined: {}'.format(rounded))
        return rounded

    def _get_part_head(self,bucket,key,version,part):
        resp = self.client.head_object(
            Bucket=bucket,
            Key=key,
            VersionId=version,
            PartNumber=int(part)
        )
        logger.debug('Part Head: {}'.format(resp))
        return resp

    def _list_multipart_uploads(self):
        resp = self.client.list_multipart_uploads(
            Bucket=self.bucket
        )
        logger.debug('List of Multipart Uploads: {}'.format(resp))
        return resp

    def _list_parts(self):
        resp = self.client.list_parts(
            Bucket=self.bucket,
            Key=self.key
        )
        logger.debug('List of Parts: {}'.format(resp))
        return resp

    def _lazy_load_helper(self, file_object):
        while True:
            data = file_object.read(self.chunk_size)
            if not data:
                break
            yield data

    def _chunk_file(self):
        with open(self.local,'rb') as f:
            part_number = 1
            parts = []
            for chunk in self._lazy_load_helper(f):
                lower = (part_number - 1) * self.chunk_size
                upper = ((part_number * self.chunk_size) - 1) if part_number < self.part_count else (self.local_size - 1)
                content_range = '{}-{}'.format(lower,upper)
                sha512 = hashlib.sha3_512(chunk).hexdigest()
                logger.debug('Part sha3-513: {}'.format(sha512))
                blake2 = hashlib.blake2b(chunk).hexdigest()
                logger.debug('Part blake2b: {}'.format(blake2))
                item = self._get_dynamo_item(sha512,blake2)
                if item:
                    first_location = item['locations']['L'][0]['M']
                    source_bucket = first_location['bucket']['S']
                    source_key = first_location['key']['S']
                    source_version = first_location['version']['S']
                    source_part_number = first_location['part']['N']
                    source_range = first_location['content_range']['S']
                    resp = self._copy_part(
                        source_bucket,
                        source_key,
                        source_version,
                        source_part_number,
                        source_range,
                        part_number
                    )
                    etag = resp['CopyPartResult']['ETag']
                else:
                    resp = self._upload_part(chunk,part_number)
                    etag = resp['ETag']
                parts.append(
                    { 
                        'ETag': etag,
                        'PartNumber': part_number,
                        'sha512': sha512,
                        'blake2': blake2,
                        'content_range': content_range
                   
                    }
                )
                part_number += 1
        return parts
        
    def _start_multipart_upload(self):
        resp = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.key
        )
        logger.debug('Start Multipart Upload: {}'.format(resp))
        return resp
      

    def _complete_multipart_upload(self):
        parts = list(
            map(
                lambda x: 
                    {
                        'ETag':x['ETag'],
                        'PartNumber':x['PartNumber']
                    },
                    self.parts
            )
        )
        logger.debug('Parts: {}'.format(parts))
        logger.debug('Parts Type: {}'.format(type(parts)))
        resp = self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            MultipartUpload={
               'Parts': parts
            },
            UploadId=self.upload_id
        )
        logger.debug('Complete Multipart Upload: {}'.format(resp))
        return resp

    def _upload_part(self,chunk,part_number):
        resp = self.client.upload_part(
           Body=chunk,
           Bucket=self.bucket,
           ContentLength=sys.getsizeof(chunk),
           Key=self.key,
           PartNumber=part_number,
           UploadId=self.upload_id
        )
        logger.debug('Upload Part: {}'.format(resp))
        return resp

    def _copy_part(self,source_bucket,source_key,source_version,source_part_number,source_range,part_number):
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
           UploadId=self.upload_id
        )
        logger.debug('Upload Part: {}'.format(resp))
        return resp

    def _put_dynamo_item(self,part_sha3,part_blake2,locations):
        self.dynamo.put_item(
            TableName=self.table_name,
            Item={
                "sha3": {"S":part_sha3},
                "blake2": {"S":part_blake2},
                "locations": {"L":locations}
            }
        )
        return True

    def _get_dynamo_item(self,part_sha3,part_blake2):
        resp = self.dynamo.get_item(
            TableName=self.table_name,
            Key={
                "sha3": {"S":part_sha3},
                "blake2": {"S":part_blake2}
            }
        ) 
        logger.debug('Part for chunk: {}'.format(resp))
        if 'Item' in resp:
            return resp['Item']
        return None

    def _push_dynamo_metadata(self):
        for item in self.parts:
            exists = self._get_dynamo_item(item['sha512'],item['blake2'])
            if exists:
                locations = exists['locations']['L']
                locations.append( {"M": {
                    "path": {"S":self.path},
                    'bucket': {"S":self.bucket},
                    'key': {"S":self.key},
                    "part": {"N":str(item['PartNumber'])}, 
                    "version": {"S":self.version},
                    "upload_id": {"S":self.upload_id},
                    "content_range": {"S":item['content_range']}
                } } )
            else:
                locations = [ {"M": {
                    "path": {"S":self.path},
                    'bucket': {"S":self.bucket},
                    'key': {"S":self.key},
                    "part": {"N":str(item['PartNumber'])}, 
                    "version": {"S":self.version},
                    "upload_id": {"S":self.upload_id},
                    "content_range": {"S":item['content_range']}
                } } ]
            self._put_dynamo_item(item['sha512'],item['blake2'],locations)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync file changes to s3')
    parser.add_argument('local', type=str, help='Local file to be synced')
    parser.add_argument('dest', type=str, help='Destination of file to be synced')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='Turn on log level debug')
    args = parser.parse_args()
    if args.debug:
        console_logger.setLevel(logging.DEBUG)
    supersync = SuperSync(args.local, args.dest, 'supersync')
