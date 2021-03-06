import boto3
import logging
from botocore.exceptions import ClientError
from itertools import repeat
from multiprocessing import Manager, Pool, Queue


logger = logging.getLogger('supersync')

def upset_dynamo_item(profile,table_name,item,bucket,key,version,upload_id,speed):
    metadata = MetaDataStore(profile,table_name,speed,check_table=False)
    exists = metadata.get_dynamo_item(item[metadata.hash_type1],item[metadata.hash_type2])
    part_metadata = {"M": {
            'bucket': {"S":bucket},
            'key': {"S":key},
            "part": {"N":str(item['PartNumber'])}, 
            "version": {"S":version},
            "upload_id": {"S":upload_id},
            "content_range": {"S":item['content_range']}
    } }
    if exists:
        locations = exists['locations']['L']
        locations.append(part_metadata)
    else:
        locations = [part_metadata]
    metadata.put_dynamo_item(item[metadata.hash_type1],item[metadata.hash_type2],locations)
    return True

class MetaDataStore(object):
    def __init__(self,profile,table_name,speed,concurrency=10,check_table=True):
        assert type(table_name) is str, "Table Name must be string type"
        session = boto3.session.Session(profile_name=profile)
        self.concurrency = concurrency
        self.dynamo = session.client('dynamodb')
        self.table_name = table_name
        self.profile = profile
        self.speed = speed
        self.hash_type1, self.hash_type2 = self._get_hash_types(speed)
        if check_table:
            self._check_or_create_table()

    def _check_or_create_table(self):
        try:
            table = self.dynamo.describe_table(TableName=self.table_name)
            assert len(table['Table']['KeySchema']) == 2,\
                "Primary Key of {} must be composite key".format(self.table_name)
            for key in table['Table']['KeySchema']:
                if key['KeyType'] == 'HASH':
                    assert key['AttributeName'] == self.hash_type1,\
                        'Partition key must be named {}'.format(self.hash_type1)
                if key['KeyType'] == 'RANGE':
                    assert key['AttributeName'] == self.hash_type2,\
                        'Partition key must be named {}'.format(self.hash_type2)
        except ClientError as e:
            logger.debug('Table {} does not exist, Creating'.format(self.table_name))
            resp = self.dynamo.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': self.hash_type2, 
                        'AttributeType': 'S'
                    }, 
                    {
                        'AttributeName': self.hash_type1, 
                        'AttributeType': 'S'
                    }
                ],
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': self.hash_type1, 
                        'KeyType': 'HASH'
                    }, 
                    {
                        'AttributeName': self.hash_type2, 
                        'KeyType': 'RANGE'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            waiter = self.dynamo.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)

    def put_dynamo_item(self,part_hash1,part_hash2,locations):
        self.dynamo.put_item(
            TableName=self.table_name,
            Item={
                self.hash_type1: {"S":part_hash1},
                self.hash_type2: {"S":part_hash2},
                "locations": {"L":locations}
            }
        )
        return True

    def get_dynamo_item(self,part_hash1,part_hash2):
        resp = self.dynamo.get_item(
            TableName=self.table_name,
            Key={
                self.hash_type1: {"S":part_hash1},
                self.hash_type2: {"S":part_hash2}
            }
        ) 
        logger.debug('Part for chunk: {}'.format(resp))
        if 'Item' in resp:
            return resp['Item']
        return None

    def _get_hash_types(self, speed):
        if speed == 'default':
            return ('sha3','blake2')
        elif speed == 'fast':
            return ('xxhash','blank')
        else:
            raise('Unknown Speed Setting')

    def push_dynamo_metadata(self,parts,bucket,key,version,upload_id):
        cap_need = (self.concurrency * 2) if self.concurrency > len(parts) else len(parts)
        table_info = self.dynamo.describe_table(
            TableName=self.table_name
        )
        write_exist = table_info['Table']['ProvisionedThroughput']['WriteCapacityUnits'] 
        read_exist = table_info['Table']['ProvisionedThroughput']['ReadCapacityUnits'] 
        if cap_need > write_exist and len(parts) > 1000:
            self.dynamo.update_table(
                TableName=self.table_name,
                ProvisionedThroughput={
                    'ReadCapacityUnits': cap_need,
                    'WriteCapacityUnits': cap_need
                }
            )
        with Pool(self.concurrency) as pool:
            pool.starmap(
                upset_dynamo_item,
                zip(
                    repeat(self.profile),
                    repeat(self.table_name),
                    parts,
                    repeat(bucket),
                    repeat(key),
                    repeat(version),
                    repeat(upload_id),
                    repeat(self.speed)
                )
            )
        if cap_need > write_exist or cap_need > read_exist:
            if len(parts) > 1000:
                self.dynamo.update_table(
                    TableName=self.table_name,
                    ProvisionedThroughput={
                        'ReadCapacityUnits': read_exist,
                        'WriteCapacityUnits': write_exist
                        }
                    )
