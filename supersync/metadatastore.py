import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger('supersync')

class MetaDataStore(object):
    def __init__(self,profile,table_name):
        assert type(table_name) is str, "Table Name must be string type"
        session = boto3.session.Session(profile_name=profile)
        self.dynamo = session.client('dynamodb')
        self.table_name = table_name
        self._check_or_create_table()

    def _check_or_create_table(self):
        try:
            table = self.dynamo.describe_table(TableName=self.table_name)
            assert len(table['Table']['KeySchema']) == 2,\
                "Primary Key of {} must be composite key".format(self.table_name)
            for key in table['Table']['KeySchema']:
                if key['KeyType'] == 'HASH':
                    assert key['AttributeName'] == 'sha3',\
                        'Partition key must be named sha3'
                if key['KeyType'] == 'RANGE':
                    assert key['AttributeName'] == 'blake2',\
                        'Partition key must be named blake2'
        except ClientError as e:
            logger.debug('Table {} does not exist, Creating'.format(self.table_name))
            resp = self.dynamo.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'blake2', 
                        'AttributeType': 'S'
                    }, 
                    {
                        'AttributeName': 'sha3', 
                        'AttributeType': 'S'
                    }
                ],
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'sha3', 
                        'KeyType': 'HASH'
                    }, 
                    {
                        'AttributeName': 'blake2', 
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

    def put_dynamo_item(self,part_sha3,part_blake2,locations):
        self.dynamo.put_item(
            TableName=self.table_name,
            Item={
                "sha3": {"S":part_sha3},
                "blake2": {"S":part_blake2},
                "locations": {"L":locations}
            }
        )
        return True

    def get_dynamo_item(self,part_sha3,part_blake2):
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

    def push_dynamo_metadata(self,parts,bucket,key,version,upload_id):
        for item in parts:
            exists = self.get_dynamo_item(item['sha512'],item['blake2'])
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
            self.put_dynamo_item(item['sha512'],item['blake2'],locations)

