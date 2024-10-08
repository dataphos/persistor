# Persistor

[![Apache 2.0 License](https://img.shields.io/github/license/dataphos/persistor)](./LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/dataphos/persistor?sort=semver)](https://github.com/dataphos/persistor/releases/latest)

Persistor is a product used for permanent data storage. It provides support for multiple cloud platforms, more specifically the Google Cloud Platform and Microsoft Azure, as well as for various broker types (GCP PubSub, Azure Service Bus and Kafka) and different kinds of storage services (Google Cloud Storage and Azure Blob Storage).

It is used to simplify the subsequent processing of data in the case of logical errors or data mapping, while it can also be used as a safety step. It serves as a backup component that works independently of the rest of the system. 

Based on our experience, it is incredibly useful to have data stored in cheap storage, such as the Google Cloud Storage, since that way data can easily be made available for different processing needs, depending on the use case.

Apart from its primary purpose of permanent data storage, the Persistor comes with additional components that offer features which could be crucial for some systems.

The official Persistor documentation is available [here](https://docs.dataphos.com/persistor/). It contains an in-depth overview of each component, a quickstart setup, detailed deployment instructions, configuration options, usage guides and more, so be sure to check it out for better understanding.

### Indexer
Indexer is an optional component used for receiving message metadata from Persistor and indexing it. 

The Indexer is essentially a Persistor that writes to MongoDB, the difference being in how it processes its incoming messages - one big JSON split into smaller documents.


### Indexer API

Indexer API is an optional component that offers different methods for querying the underlying Mongo database, maintained by the [Indexer](#indexer) component. In order to use the Indexer API, the system needs to already have a running version of both the [core Persistor](#persistor) component and the [Indexer](#indexer) component.

Although it is possible for a user to send requests directly to the Indexer API, this component is the most beneficial in combination with the [Resubmitter API](#resubmitter-api).

### Resubmitter API

Resubmitter API is an optional component that works in combination with [Indexer API](#indexer-api) to reconstruct the original message and send it to the broker, in the same way that the original message producer would do. In order to use the Resubmitter API, all of the previously mentioned components need to be deployed and running.

This process is quite beneficial for systems that tend to lose messages due to unreliable processing pipelines or need to reprocess a batch of messages due to an error in the processing logic.

## Installation
In order to use Persistor as a whole with all of its components, the only major requirement from the user is to have a running project on one of the two major cloud providers: GCP or Azure.

All of the other requirements for the product to fully-function (message broker instance, cloud storage bucket, identity and access management of the particular cloud) are further explained and can be analyzed in the [Quickstart section](https://docs.dataphos.com/persistor/quickstart/) of its official documentation.

## Features
### Persistor Core
- The core component reads messages from a given broker and stores them to the chosen storage in **raw** format.
  - The folder structure of the landing cloud storage is highly configurable.
  - Each message is stored as a separate object, with content identical to the payload of the brokers' message.
- It also has a dead letter topic for messages that couldn't be processed, stating the cause of the error.

### Indexer
- The Indexer continuously receives messages from a dedicated topic.
- It reads the messages, validates and converts values into suitable formats and transforms messages into predefined structures.
- Each processed message is stored in the MongoDB instance as a separate object.

### Indexer API
- The Indexer API offers different methods for querying the underlying Mongo database to fetch any type of metadata related to the messages, e.g. fetching all, some, within specific timestamps, based on specific metadata, etc.


### Resubmitter API
- The Resubmitter API connects to the Indexer API to fetch the metadata necessary for the resubmission, while also connecting to a permanent storage for retrieving the payloads and to the message service for publishing reconstructed messages, to offer different methods for the resubmission of messages, e.g. resubmitting all, some, within specific timestamps, based on specific metadata, etc.


## Contributing
For all the inquiries regarding contributing to the project, be sure to check out the information in the [CONTRIBUTING.md](CONTRIBUTING.md) file.

## License
This project is licensed under the [Apache 2.0 License](LICENSE).
