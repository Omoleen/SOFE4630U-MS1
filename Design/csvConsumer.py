from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import json
import os

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="clarity-staging-4afe8";
topic_name = "weatherLabels";   # change it for your topic name if needed
subscription_id = "weatherLabels-sub";   # change it for your subscription name if needed

# create a subscriber to the subscription of the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id,topic_name);

print(f"Listening for messages on {subscription_path}..\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # convert from bytes to dictionary (deserialization)
    record = json.loads(message.data.decode('utf-8'));

    # print the values of the dictionary. The callback runs on several threads at
    # once, so the whole record is built as one string and printed in a single
    # call, otherwise the fields of different records interleave on the screen.
    lines=["Consumed record:"];
    for field, value in record.items():
        lines.append("   {} : {}".format(field, value));
    print("\n".join(lines))

    # Report to Google Pub/Sub the successful processing of the received message
    message.ack()

with subscriber:
    # The callback function will be called for each message received from the topic
    # through the subscription.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
