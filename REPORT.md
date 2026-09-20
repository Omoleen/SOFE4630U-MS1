# SOFE4630U Milestone 1 Report
## Data Ingestion System with Google Cloud Pub/Sub

**Name:** Emmanuel Omole  
**Student number:** 101004432  
**Date:** 2026-09-20  

**GitHub repository:** https://github.com/Omoleen/SOFE4630U-MS1

**Video 1, the smart meter application:** https://www.loom.com/share/64997bce25654d81a5e3f1c8fac8440e

**Video 2, the design part:** https://www.loom.com/share/20a5b90d97384cbc888ef39cb1a7caee

---

## 1. Environment

| Item | Value |
| --- | --- |
| GCP project ID | `clarity-staging-4afe8` |
| Service account | `pubsub-system@clarity-staging-4afe8.iam.gserviceaccount.com` |
| Roles granted | Pub/Sub Publisher, Pub/Sub Subscriber |
| Python | 3.13 |
| Libraries | `google-cloud-pubsub`, `numpy` |

The service account key is a JSON file kept in the same folder as each script.
Every script locates it with `glob.glob("*.json")` and assigns it to the
`GOOGLE_APPLICATION_CREDENTIALS` environment variable, which the client library
reads when it opens a connection. The key is listed in `.gitignore` so it is
never pushed to GitHub.

Three topics were created, each with its default pull subscription.

| Topic | Subscription | Message payload |
| --- | --- | --- |
| `testTopic` | `testTopic-sub` | UTF-8 string |
| `smartMeter` | `smartMeter-sub` | JSON object |
| `weatherLabels` | `weatherLabels-sub` | JSON object |

---

## 2. Producer and consumer with a Python script

`v1/producer.py` builds a `PublisherClient`, resolves the full topic path, then
loops up to 100 times reading a line from the keyboard. Each string is encoded to
bytes (serialization) and handed to `publisher.publish()`. The call is
asynchronous, so `future.result()` blocks until Pub/Sub acknowledges the write,
which turns a lost message into an exception instead of a silent failure. An
empty line ends the loop.

`v1/consumer.py` builds a `SubscriberClient` and registers a callback against
`testTopic-sub`. The library pulls messages in a background thread and invokes
the callback once per message. The callback decodes the payload back to a string
(deserialization), prints it, and calls `message.ack()` so Pub/Sub stops
redelivering it.

Both scripts running against the live topic:

```
$ python producer.py
Published messages with ordering keys to projects/clarity-staging-4afe8/topics/testTopic.
Enter a value (String):hello from Emmanuel 101004432
Producing a record: b'hello from Emmanuel 101004432'
Enter a value (String):EDA lab milestone 1
Producing a record: b'EDA lab milestone 1'
Enter a value (String):
```

```
$ python consumer.py
Listening for messages on projects/clarity-staging-4afe8/subscriptions/testTopic-sub..

Consumed record with value : b'hello from Emmanuel 101004432'
Consumed record with value : b'EDA lab milestone 1'
```

---

## 3. Simulating a smart meter

`v2/smartMeter.py` publishes to the `smartMeter` topic. Instead of reading the
keyboard it generates a measurement every 0.5 seconds from one of three device
profiles (`boston`, `denver`, `losang`), each defined by the mean and standard
deviation of temperature, humidity and pressure. `numpy.random.normal()` draws a
value from that distribution and the result is clamped to a physically sensible
range. Roughly one field in ten is replaced by `None` to imitate a sensor that
drops a reading, which gives the downstream processing something realistic to
handle.

The message is a dictionary, so it is serialized with `json.dumps()` before being
encoded to bytes. `v2/consumer.py` mirrors that: `json.loads()` turns the payload
back into a dictionary rather than a plain string. That single change is the
whole difference from the version 1 consumer.

Both scripts running against the `smartMeter` topic:

```
$ python smartMeter.py
Published messages with ordering keys to projects/clarity-staging-4afe8/topics/smartMeter.
The messages {'time': 1789924964.7622578, 'profile_name': 'losang', 'temperature': 66.54883074170864, 'humidity': 50.97478699815249, 'pressure': 1.6970458792518206} has been published successfully
The messages {'time': 1789924965.477462, 'profile_name': 'boston', 'temperature': None, 'humidity': 82.74950344459431, 'pressure': 0.8228826902670597} has been published successfully
The messages {'time': 1789924966.026942, 'profile_name': 'denver', 'temperature': 47.194293436027465, 'humidity': 36.41717693836052, 'pressure': 1.4729992564270717} has been published successfully
```

```
$ python consumer.py
Listening for messages on projects/clarity-staging-4afe8/subscriptions/smartMeter-sub..

Consumed record with value : {'time': 1789924964.7622578, 'profile_name': 'losang', 'temperature': 66.54883074170864, 'humidity': 50.97478699815249, 'pressure': 1.6970458792518206}
Consumed record with value : {'time': 1789924965.477462, 'profile_name': 'boston', 'temperature': None, 'humidity': 82.74950344459431, 'pressure': 0.8228826902670597}
Consumed record with value : {'time': 1789924966.026942, 'profile_name': 'denver', 'temperature': 47.194293436027465, 'humidity': 36.41717693836052, 'pressure': 1.4729992564270717}
```

The second reading has `'temperature': None`, the dropped field, and it survives
the round trip unchanged because `json` maps it to `null` and back.

---

## 4. Discussion

### 4.1 What is EDA? What are its advantages and disadvantages?

Event driven architecture is a style in which components communicate by emitting
and reacting to events rather than calling each other directly. A producer
publishes a fact about something that happened, such as a reading of 31 degrees
from meter 42, to a broker. Consumers that care about that fact subscribe to
it. The producer does not know who the consumers are, how many there are, or
whether any exist at all. The broker, which is Cloud Pub/Sub here, holds the
event until every subscription has acknowledged it.

Advantages:

- **Loose coupling.** Producers and consumers share only the message schema. A
  new consumer can be added without touching or redeploying the producer.
- **Independent scaling.** Each side scales on its own load. Ten consumer
  instances can share one subscription to drain a backlog while the producer
  stays at one instance.
- **Resilience.** If a consumer crashes, the broker retains unacknowledged
  messages and redelivers them, and the producer keeps publishing throughout.
  The same buffer absorbs traffic spikes, so a slow consumer causes a queue
  rather than dropped data.
- **Real time reaction.** Events are pushed as they happen instead of waiting for
  a batch window, which suits telemetry, fraud detection and monitoring.

Disadvantages:

- **Harder to reason about.** There is no single call stack to follow. Working
  out what a published event ultimately triggered means tracing it across
  services.
- **Eventual consistency.** Delivery is asynchronous, so a consumer's view lags
  the producer's. Workflows that need an immediate answer fit request/response
  better.
- **Weaker delivery guarantees.** A message can arrive more than once, for
  instance when an acknowledgement is lost, and messages can arrive out of
  order. Consumers have to be idempotent, and ordering costs extra.
- **Operational overhead.** The broker is extra infrastructure to run, secure and
  monitor, and schema changes must stay compatible with consumers that have not
  been updated yet.

### 4.2 Push and pull subscriptions

**Pull.** The subscriber opens a connection to Pub/Sub and asks for messages,
either with repeated `pull` requests or with the streaming pull used by
`subscriber.subscribe()` in this milestone. The subscriber controls the rate: it
requests work when it has capacity, processes it, then acknowledges.

Strengths: the consumer sets its own pace, so a slow processing step creates a
backlog in the broker instead of overwhelming the consumer. Batching many
messages per request gives high throughput at low cost per message. The consumer
needs no public endpoint or TLS certificate, so it can sit behind a firewall, on
a laptop or on an on-premise machine. Horizontal scaling is easy because several
workers can pull from one subscription.

Weaknesses: the consumer must be running and holding an open connection, which
does not suit code that only exists while handling a request. It also has to
manage its own retry, flow control and acknowledgement deadlines.

Pull suits a data pipeline worker on GKE or Compute Engine draining a high
volume meter feed, which is the shape of this milestone.

**Push.** Pub/Sub sends each message as an HTTPS POST to an endpoint registered
on the subscription. A 2xx response counts as the acknowledgement; anything else
triggers redelivery with exponential backoff.

Strengths: the consumer is a plain web handler, so it fits serverless platforms
such as Cloud Run and Cloud Functions and can scale to zero when idle. No client
library, no long lived connection and no polling loop. Several different systems,
including ones outside GCP, can receive events simply by exposing a URL.

Weaknesses: the endpoint must be publicly reachable over HTTPS with a valid
certificate, which is a problem for private networks. Pub/Sub controls the rate,
so a burst can overrun a consumer that cannot scale fast enough. Throughput per
subscription is lower than pull because delivery is one request per message, and
authenticating the incoming requests is extra work.

Push suits a Cloud Run service that sends an alert whenever a meter reports a
temperature above a threshold. Traffic is low, the work per event is small, and
paying for an always on puller would be waste.

### 4.3 Ordering keys

Pub/Sub does not guarantee order by default. Messages may be produced from
different publisher instances, routed through different regions and retried
independently, so a consumer can see them in any sequence. An ordering key fixes
that for messages that belong together. When a publisher attaches an ordering key
and the subscription has message ordering enabled, Pub/Sub delivers all messages
carrying the same key to the same subscriber in the order they were published. It
does not order messages across different keys, which is what keeps the guarantee
affordable.

The key is chosen as the identifier of the entity whose history matters. Useful
examples:

- **Per meter telemetry.** Using the meter ID as the ordering key means readings
  from meter `denver-07` arrive in timestamp order, so a consumer computing a
  rolling average or a rate of change never sees a reading go backwards in time.
  Readings from `boston-03` are ordered independently and are free to arrive in
  parallel.
- **Account balance updates.** With the account ID as the key, a deposit followed
  by a withdrawal is applied in that order. Reversed, the withdrawal could be
  rejected for insufficient funds even though the deposit had already been made.
- **Change data capture.** Replicating a database table, the primary key of the
  row is the ordering key, so `INSERT`, then `UPDATE`, then `DELETE` for one row
  reach the replica in the order they happened. Without it a late `UPDATE` could
  resurrect a deleted row.

Benefits: the consumer can be written as a simple sequential state machine, with
no buffering, sequence numbers or reordering logic, and no need to detect and
discard stale updates. Because ordering is scoped per key, throughput stays high
across the whole topic. The cost is that messages sharing one key are handled
serially, so a poison message blocks its own key until it is acknowledged or dead
lettered. A key chosen too coarsely is worse: one key shared by the whole fleet
turns the topic into a single serial queue.

---

## 5. Design: publishing and consuming the CSV records

The design part moves 100 labelled weather records from `Labels.csv` through a
topic named `weatherLabels`. The name describes the payload, which is a labelled
weather observation, rather than the mechanism.

Each CSV row has five fields.

| Field | Meaning |
| --- | --- |
| `time` | Unix timestamp of the observation |
| `profileName` | Device profile that produced it: `boston`, `denver` or `losang` |
| `temperature` | Temperature reading |
| `humidity` | Relative humidity reading |
| `pressure` | Pressure reading |

### 5.1 Producer

`Design/csvProducer.py` opens the file with `csv.DictReader`, which yields one
dictionary per row already keyed by the header names. `DictReader` returns every
value as text, so the four numeric fields are cast to `float` before publishing.
Without that step a consumer would receive `"31.11"` rather than `31.11` and
would have to parse the numbers itself. The dictionary is serialized with
`json.dumps()`, encoded to UTF-8 bytes and published. As in the earlier scripts,
`future.result()` confirms each publish, and the publish is wrapped in a
`try`/`except` so one rejected record does not abort the remaining rows. The
script reports the total at the end.

### 5.2 Consumer

`Design/csvConsumer.py` subscribes to `weatherLabels-sub`. The callback decodes
the payload and runs `json.loads()` to recover the dictionary, then iterates over
its items and prints each field with its value. Acknowledging inside the callback
means a record is only removed from the subscription after it has been printed,
so a crash mid-run causes redelivery rather than data loss.

### 5.3 Two problems found while running it

**Missing readings.** Sixteen of the hundred rows have an empty cell, because the
file was produced by the same kind of meter that drops about one field in ten.
Casting those cells with `float()` raised
`ValueError: could not convert string to float: ''` and stopped the producer on
row four. A `toNumber()` helper now returns `None` for an empty cell, which
matches what `smartMeter.py` publishes for a dropped reading and serializes to
`null` in JSON.

**Interleaved output.** The subscriber library runs the callback on several
threads at once. Printing one line per field let the fields of two records
interleave on screen, producing lines such as
`temperature : 34.02   temperature : None`. The callback now builds the whole
record as one string and prints it in a single call. This is a property of the
consumer, not of Pub/Sub: the records themselves were never corrupted.

### 5.4 Result

```
$ python csvProducer.py
Publishing the records of Labels.csv to projects/clarity-staging-4afe8/topics/weatherLabels.
The message {'time': 1768708698.4966547, 'profileName': 'denver', 'temperature': 31.111990786610825, 'humidity': 37.483258996647876, 'pressure': 1.3481959720663381} has been published successfully
The message {'time': 1768708698.4966938, 'profileName': 'boston', 'temperature': 37.68765678165221, 'humidity': None, 'pressure': 0.919670354687257} has been published successfully
...
Done. 100 records were published to weatherLabels.
```

```
$ python csvConsumer.py
Listening for messages on projects/clarity-staging-4afe8/subscriptions/weatherLabels-sub..

Consumed record:
   time : 1768708698.4966547
   profileName : denver
   temperature : 31.111990786610825
   humidity : 37.483258996647876
   pressure : 1.3481959720663381
Consumed record:
   time : 1768708698.4966938
   profileName : boston
   temperature : 37.68765678165221
   humidity : None
   pressure : 0.919670354687257
...
```

All 100 records were published and all 100 were received.

One note on counting them. An earlier run was stopped with the consumer holding
unacknowledged messages, and when the consumer was restarted Pub/Sub redelivered
them, so 107 records were printed for 100 published. That is at least once
delivery working as designed, not a bug. The figures above come from a run
against a freshly created subscription.

---

## 6. Conclusion

None of the three producers in this milestone referenced a consumer, and the
consumers were started and stopped without the publishers noticing. Going from
version 1 to version 2 showed how little the transport cares about the payload:
string to dictionary was two lines, `json.dumps()` on one side and
`json.loads()` on the other, with the topic and the subscription untouched. The
design part reused the pattern on a third topic. That is the practical argument
for event driven architecture: adding a producer or a consumer is a local
change.
