# SOFE4630U Milestone 1: Data Ingestion System (Cloud Pub/Sub)

**Name:** Emmanuel Omole
**Student number:** 101004432

Publishers and subscribers for Google Cloud Pub/Sub, written in Python.
GCP project: `clarity-staging-4afe8`.

## Layout

| Path | Purpose |
| --- | --- |
| `v1/producer.py` | Reads strings from the keyboard and publishes them to `testTopic`. |
| `v1/consumer.py` | Subscribes to `testTopic-sub` and prints every string it receives. |
| `v2/smartMeter.py` | Simulates a smart meter, publishing random JSON measurements to `smartMeter`. |
| `v2/consumer.py` | Subscribes to `smartMeter-sub` and prints the deserialized measurements. |
| `Design/csvProducer.py` | Reads `Labels.csv`, converts each row to a dictionary and publishes it to `weatherLabels`. |
| `Design/csvConsumer.py` | Subscribes to `weatherLabels-sub` and prints the field values of each record. |
| `Design/Labels.csv` | 100 labelled weather records used by the design part. |
| `REPORT.md` | Milestone report: setup, discussion and design. |

## Topics and subscriptions

| Topic | Subscription | Payload |
| --- | --- | --- |
| `testTopic` | `testTopic-sub` | UTF-8 string |
| `smartMeter` | `smartMeter-sub` | JSON object |
| `weatherLabels` | `weatherLabels-sub` | JSON object |

## Running the scripts

1. Create a service account with the **Pub/Sub Publisher** and **Pub/Sub Subscriber**
   roles, download a JSON key and copy it into the folder of the script you want to
   run. Each script calls `glob.glob("*.json")` and points
   `GOOGLE_APPLICATION_CREDENTIALS` at the first key it finds in the working
   directory.
2. Install the client library:

   ```shell
   pip install google-cloud-pubsub numpy
   ```

3. Set `project_id` in the script if you are using a different GCP project.
4. Start the consumer first, then the producer, each in its own terminal:

   ```shell
   cd Design
   python csvConsumer.py     # terminal 1
   python csvProducer.py     # terminal 2
   ```

The key file is excluded by `.gitignore` and is not part of this repository.
