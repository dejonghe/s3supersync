#!/usr/bin/env python3.6
import argparse
import boto3
import hashlib
import math
import os
import sys
from botocore.exceptions import ClientError
from itertools import repeat
from multiprocessing import Manager, Pool, Queue
from random import randrange
from lib.logger import logging, logger, console_logger
from lib.metadatastore import MetaDataStore
from lib.s3wrapper import S3Wrapper

KB = 1024
MB = KB * KB
GB = MB * KB

def process_part(
        local,
        dest,
        chunk_size,
        local_size,
        part_count,
        part_number,
        upload_id,
        profile,
        table_name,
        queue):
    pid = os.getpid()
    part_number = part_number + 1
    metadata = MetaDataStore(profile,table_name)
    s3_wrapper = S3Wrapper(profile,local,dest)
    with open(local,'rb') as file_object:
        file_object.seek((part_number - 1) * chunk_size)
        chunk = file_object.read(chunk_size)
        lower = (part_number - 1) * chunk_size
        upper = ((part_number * chunk_size) - 1) if part_number < part_count else (local_size - 1)
        content_range = '{}-{}'.format(lower,upper)
        sha512 = hashlib.sha3_512(chunk).hexdigest()
        logger.debug('{}: Part sha3-513: {}'.format(pid,sha512))
        blake2 = hashlib.blake2b(chunk).hexdigest()
        logger.debug('{}: Part blake2b: {}'.format(pid,blake2))
        item = metadata.get_dynamo_item(sha512,blake2)
        if item:
            index = 0
            if len(item['locations']['L']) > 1:
                index = randrange(0,(len(item['locations']['L']) - 1))
            first_location = item['locations']['L'][index]['M']
            source_bucket = first_location['bucket']['S']
            source_key = first_location['key']['S']
            source_version = first_location['version']['S']
            source_part_number = first_location['part']['N']
            source_range = first_location['content_range']['S']
            resp = s3_wrapper.copy_part(
                source_bucket,
                source_key,
                source_version,
                source_part_number,
                source_range,
                part_number,
                upload_id
            )
            etag = resp['CopyPartResult']['ETag']
        else:
            resp = s3_wrapper.upload_part(
               chunk,
               part_number,
               upload_id
            )
            logger.debug('{}: Upload Part: {}'.format(pid,resp))
            etag = resp['ETag']
        queue.put(
            { 
                'ETag': etag,
                'PartNumber': part_number,
                'sha512': sha512,
                'blake2': blake2,
                'content_range': content_range
            }
        )

class SuperSync(object):

    def __init__(self,profile,table_name,local,dest,concurrency):
        self.profile = profile
        self.metadata = MetaDataStore(profile,table_name)
        self.s3 = S3Wrapper(profile,local,dest)
        self.concurrency = concurrency
        self.local = self.s3.local
        self.dest = self.s3.dest
        self.local_size = os.path.getsize(self.local)
        logger.debug(self.local_size)
        self.chunk_size = self._get_chunk_size()
        self.part_count = self._get_part_count()

    def sync(self):
        upload_id = self.s3.start_multipart_upload()
        parts = self._chunk_file(upload_id)
        upload = self.s3.complete_multipart_upload(upload_id,parts)
        version = upload['VersionId']
        self.metadata.push_dynamo_metadata(
            parts,
            self.s3.bucket,
            self.s3.key,
            version,
            upload_id
        )
     
    def _get_chunk_size(self):
        prior_chunk_size = self.s3.get_object_chunk_size()
        if prior_chunk_size:
            chunk_size = prior_chunk_size
            logger.debug('Prior object found, part size: {}MB'.format(chunk_size/MB))
        else:
            chunk_size = 8 * MB
            logger.debug('Prior object not found, trying part size: {}MB'.format(chunk_size/MB))
        while self.local_size / chunk_size > 10000:
            logger.debug('Part size {}MB too small'.format(str(chunk_size/MB)))
            chunk_size = chunk_size * 2
        logger.debug('Part size determined: {}MB'.format(str(chunk_size/MB)))
        return chunk_size
        

    def _get_part_count(self):
        unrounded = self.local_size / self.chunk_size
        rounded = int(math.ceil(unrounded))
        logger.debug('Number of parts determined: {}'.format(rounded))
        return rounded

    def _lazy_load_helper(self, file_object):
        while True:
            data = file_object.read(self.chunk_size)
            if not data:
                break
            yield data

    def _queue_get_all(self,queue):
        items = []
        while len(items) < self.part_count:
            items.append(queue.get())
        return items

    def _chunk_file(self,upload_id):
        manager = Manager()
        queue = manager.Queue()
        with Pool(self.concurrency) as pool:
            pool.starmap(
                process_part,
                zip(
                    repeat(self.local),
                    repeat(self.dest),
                    repeat(self.chunk_size),
                    repeat(self.local_size),
                    repeat(self.part_count),
                    range(self.part_count),
                    repeat(upload_id),
                    repeat(self.profile),
                    repeat(self.metadata.table_name),
                    repeat(queue)
                )
            )
            logger.debug('Parts processed, pool not closed')
        logger.debug('Parts processed, waiting for queue items')
        return self._queue_get_all(queue)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync file changes to s3')
    parser.add_argument('local', type=str, help='Local file to be synced')
    parser.add_argument('dest', type=str, help='Destination of file to be synced')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='Turn on log level debug')
    parser.add_argument('-p', '--profile', dest='profile', default='default', help='AWS Profile to use.')
    parser.add_argument('-c', '--concurrency', dest='concurrency', type=int, default=10, help='Number of processes to use.')
    parser.add_argument('-t', '--table_name', dest='table_name', type=str, default='supersync', help='DynamoDB table name too use.')
    args = parser.parse_args()
    if args.debug:
        console_logger.setLevel(logging.DEBUG)
    supersync = SuperSync(args.profile,args.table_name,args.local,args.dest,args.concurrency)
    supersync.sync()
