import aioboto3
import botocore.exceptions


async def ensure_table_exists(session: aioboto3.Session, table_name: str, region: str):
    async with session.client("dynamodb", region_name=region) as client:
        try:
            await client.describe_table(TableName=table_name)
        except botocore.exceptions.ClientError:
            await client.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "id", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )

            waiter = client.get_waiter("table_exists")
            await waiter.wait(TableName=table_name)
