
import os
import math
import boto3


crawl_id = "f0d933c9-ae73-430c-b7df-f331c379cbd6"


total_messages = 2123698


client = boto3.client('sqs',
                      aws_access_key_id=os.environ["aws_access"],
                      aws_secret_access_key=os.environ["aws_secret"],
                      region_name='us-east-1')
print(f"Creating queue for crawl_id: {crawl_id}")

test_queue = client.create_queue(QueueName=f"zoa_crawls")

# if test_queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
test_queue_url = test_queue["QueueUrl"]


response = client.get_queue_url(
            QueueName=f'crawl_{crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )
val_queue_url = response["QueueUrl"]


for i in range(math.ceil(total_messages/10)):

    # print(i)
    # TODO:  Pull from the crawl_id queue.
    sqs_response = client.receive_message(
        QueueUrl=val_queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    delete_batch = []
    message_batch = []
    if len(sqs_response["Messages"]) > 0:

        for i in range(len(sqs_response["Messages"])): 

            # print(i)
            message = sqs_response["Messages"][i]

            id = message['MessageId']
            body = message['Body']

            # print(body)

            # print(type(body))
            # exit()

            new_msg = {"Id": id, "MessageBody": body}
            message_batch.append(new_msg)

        del_info = {'ReceiptHandle': message["ReceiptHandle"],
                    'Id': message["MessageId"]}
        delete_batch.append(del_info) 

        response1 = client.send_message_batch(QueueUrl=test_queue_url,
                                              Entries=message_batch)

        response2 = client.send_message_batch(QueueUrl=val_queue_url,
                                              Entries=message_batch)

        # We need to delete from the real queue because otherwise we'll continuously select same file
        response = client.delete_message_batch(
            QueueUrl=val_queue_url,
            Entries=delete_batch)

