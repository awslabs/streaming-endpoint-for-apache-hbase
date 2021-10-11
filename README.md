# HBase Custom Replication Endpoint for Kinesis
## Overview
Since HBase 0.98.9 we can create custom replication endpoints. In this example we create a custom replication endpoint to replicate table edits to AWS Kinesis streams, Apache Kafka and AWS MSK.  

## Usage
### Cluster
1. `mvn clean package`
2. Configure HBase table to stream mapping in hbase-site.xml as explained in "Supported Hbase configuration properties" section.
3. Copy jar to hbase classpath
4. Place the jar in HBase master and region servers classpath. This can be configured in /etc/hbase/conf/hbase-env.sh 
5. Restart region servers and the Hbase Master.
6. Add the Custom replication peer in hbase shell:
`add_peer 'Kinesis_endpoint', ENDPOINT_CLASSNAME => 'com.amazonaws.hbase.StreamingReplicationEndpoint'`

Using the `hbase shell` enter the following commands to define the table:
```
create 'test-table', {NAME => 'cf', REPLICATION_SCOPE=>'1'}
put 'test-table', 'a', 'cf:q', 'v'
disable 'test-table'
drop 'test-table'
```

There are 2 implamentations: 

For Kinesis Streams: com.amazonaws.hbase.datasink.KinesisDataSinkImpl

For Kafka: com.amazonaws.hbase.datasink.KafkaDataSinkImpl

In case of Kakfa, either an MSK arn should be provided, or the '''bootstrap.servers''' attribute should be cofigured. If both configure, the '''bootstrap.servers''' will take priority.

### Unit Tests
1. `mvn clean test`

### Supported Hbase configuration properties
```
hbase.replication.bulkload.enabled
Required: no
Type: Boolean
Default: no
Description: If the WAL record for bulk loads should be replicated 

hbase.replication.drop.on.deleted.columnfamily
Required: no
Type: Boolean
Default: no
Description: If the WAL records for the drop columnFamilies should be replicated

hbase.replication.drop.on.deleted.table
Required: no
Type: Boolean
Default: no
Description: If the WAL records for the drop tables should be replicated

hbase.replication.drop.on.deleted.table
Required: no
Type: Boolean
Default: no
Description: If the WAL records for the drop tables should be replicated

hbase.replication.sink-factory-class
Required: yes
Type: String
Default: null
Example: com.amazonaws.hbase.datasink.KinesisDataSinkImpl

hbase.replication.compression-enabled
Required: no
Type: Boolean
Default: false

hbase.replication.kafka.topic-table-map
Required: yes
Type: String
Default: table1:topic1,table2:topic2

hbase.replication.kafka.bootstrap.servers
Required: no
Type: String
Default: hosname:port,hostname:port

hbase.replication.kafka.msk.arn
Required: no
Type: String
Default: null
Example: arn:aws:kafka:$REGION:$ACCOUNID:cluster/$CLUSTERNAME/$UNIQID

hbase.replication.kafka.batch.size
Description: batch.size
Required: no
Type: String
Default: 16384

hbase.replication.kafka.request.timeout.ms
Description: request.timeout.ms
Required: no
Type: String
Default: 120000

hbase.replication.kafka.retries
Descriptin: retries
Required: no
Type: String
Default: 30000

hbase.replication.kafka.retry.backoff.ms
Description: retry.backoff.ms
Required: no
Type: String
Default: 100

hbase.replication.kafka.transaction.timeout.ms
Required: no
Type: String
Description:  transaction.timeout.ms 
Default: 60000

hbase.replication.kafka.security.protocol
Required: no
Type: String
Description: security.protocol
Default: PLAINTEXT

hbase.replication.kafka.ssl.truststore.location
Required: no
Type: String
Description: ssl.truststore.location
Default: null

hbase.replication.kafka.ssl.truststore.password
Required: no
Type: String
Default: null
Description: ssl.truststore.password

hbase.replication.kafka.ssl.keystore.location
Required: no
Type: String
Description: ssl.keystore.location
Default: null

hbase.replication.kafka.ssl.key.password
Required: no
Type: String
Default: null
Description: ssl.key.password

hbase.replication.kafka.topic.create
Required: no
Type: Boolean
Defualt: false
Description: Automatically create the Kafka topic if doesn't exist

hbase.replication.kafka.topic.partitions
Required: no
Type: Short
Description: If w are creating the topic, how many partitions on the topic. 
Default: 1

hbase.replication.kafka.topic.replication-factor
Required: no
Type: String
Description: If we are creating the topic, what should be the replication factor
Default: 2

hbase.replication.kinesis.stream-table-map
Required: yes
Type: String
Default: null
Decription: Comma seperated table : Kinesis stream mapping 
Example: table1:stream1,table2:stream2

hbase.replication.kinesis.region
Required: no
Type: String
Description: The Kinesis streams region
Default: The EC2 instance region queried by SDK. 

hbase.replication.kinesis.max-connection
Required: no
Type: Integer
Default: 5
Description: Maximum rumber of KPL connections to the stream, per regionserver

hbase.replication.kinesis.request-timeout
Required: no
Type: long
Default: 60000
Description: KPL request-timeout

hbase.replication.kinesis.aggregation-enabled
Required: no
Type: Boolean
Default: false
Description: enable KPL aggregation

hbase.replication.kinesis.endpoint
Required: no
Type: String
Description: the hostname of Kinesis endpoint.
Default: kinesis.$REGION.amazonaws.com 

hbase.replication.kinesis.endpoint-port
Required: no
Type: Integer
Default: 443
Description: The http/https port of Kinesis Endpoint

hbase.replication.kinesis.cw-endpoint-port
Required: no
Type: Integer
Default: 443
Description: The http/https port of CloutWatch Metrics endpoint

hbase.replication.kinesis.cw-endpoint
Required: no
Type: String
Description: the hostname of CloudWatch Metrics endpoint for KPL metrics.
Default: monitoring.$REGION.amazonaws.com 

hbase.replication.kinesis.verify-ssl-cert
Required: no
Type: Boolean
Description: verify ssl certificate
Default: yes

hbase.replication.kinesis.metric-level
Required: no
Type: String
Description: KPL metric. valid Values (NONE, SUMMARY, and DETAILED) 
Default: NONE

hbase.replication.kinesis.log-level
Required: no
Type: string
Description: KPL LogLevel. valid Values info|warning|error) 
Default: info

hbase.replication.kinesis.record-ttl
Required: no
Type: integer
Description: KPL TTL of records. Any record older than the ttl will be discarded. 
Default: Integer.MAX_VALUE

```

### Sample replicated records
#### BulkLoad replication 
If bulkload replication is set to enabled, hbase.replication.cluster.id had to also be set. The replicated bulkload record will have HBASE::BULK_LOAD as qualifier. 
```
{
    "key": {
        "writeTime": 1632290111532,
        "sequenceId": 10,
        "tablename": "tablename",
        "nonce": 0,
        "nonceGroup": 0,
        "origLogSeqNum": 0,
        "encodedRegionName": "MjBlNTg3NDZhOTIyODc1NTdjZjk4M2NmZjg1NDc5OGI=",
        "writeEntry": null
    },
    "edit": {
        "cells": [
            {
                "qualifier": "SEJBU0U6OkJVTEtfTE9BRA==",
                "value": "ChQKB2RlZmF1bHQSCXRhYmxlbmFtZRIgMjBlNTg3NDZhOTIyODc1NTdjZjk4M2NmZjg1NDc5OGIaPwoGY2ZkZW1vEgZjZmRlbW8aKTZmZjUyYmU2MDk0ZTQ3MjViYTUyMDQ3ZWJhNGU2NjM1X1NlcUlkXzlfILqhAiAJKiQ5N2I5ZGFjYS1jMDRhLTQyOGQtODA4ZC1hOWI0MDVjOTBkMWYwAQ==",
                "type": "Put",
                "family": "TUVUQUZBTUlMWQ==",
                "timeStamp": 1632290111532,
                "row": "AA=="
            }
        ],
        "families": [
            "TUVUQUZBTUlMWQ=="
        ],
        "replay": false,
        "metafamily": "TUVUQUZBTUlMWQ=="
    }
}
```

#### Put
```
{
    "key": {
        "writeTime": 1629871473862,
        "sequenceId": 4,
        "tablename": "table",
        "nonce": 0,
        "nonceGroup": 0,
        "origLogSeqNum": 0,
        "encodedRegionName": "OTc4YjY3Mzg5YjRhZmMzYzU0NGRiMDhkMmIyMDYyYzM=",
        "writeEntry": null
    },
    "edit": {
        "cells": [
            {
                "qualifier": "Y29sdW1uZmFtaWx5",
                "value": "dmFsdWU=",
                "type": "Put",
                "family": "Y29sdW1uZmFtaWx5",
                "timeStamp": 1629871473862,
                "row": "cm93"
            }
        ],
        "families": [
            "Y29sdW1uZmFtaWx5"
        ],
        "replay": false,
        "metafamily": "TUVUQUZBTUlMWQ=="
    }
}
```

#### deleteall
```
{
    "key": {
        "writeTime": 1632213370846,
        "sequenceId": 16,
        "tablename": "tablename",
        "nonce": 0,
        "nonceGroup": 0,
        "origLogSeqNum": 0,
        "encodedRegionName": "MTdjNTcxMzIwMDMwNzk0MDIwNzAzNGM0ODFjYTViM2Q=",
        "writeEntry": null
    },
    "edit": {
        "cells": [
            {
                "qualifier": "bmFtZQ==",
                "value": "",
                "type": "DeleteColumn",
                "family": "Y2ZkZW1v",
                "timeStamp": 1632213370846,
                "row": "Mg=="
            }
        ],
        "families": [
            "Y2ZkZW1v"
        ],
        "replay": false,
        "metafamily": "TUVUQUZBTUlMWQ=="
    }
}
```
#### delete
```
{
    "key": {
        "writeTime": 1632213209128,
        "sequenceId": 15,
        "tablename": "tablename",
        "nonce": 0,
        "nonceGroup": 0,
        "origLogSeqNum": 0,
        "encodedRegionName": "MTdjNTcxMzIwMDMwNzk0MDIwNzAzNGM0ODFjYTViM2Q=",
        "writeEntry": null
    },
    "edit": {
        "cells": [
            {
                "qualifier": "bmFtZQ==",
                "value": "",
                "type": "Delete",
                "family": "Y2ZkZW1v",
                "timeStamp": 1632213048484,
                "row": "MQ=="
            }
        ],
        "families": [
            "Y2ZkZW1v"
        ],
        "replay": false,
        "metafamily": "TUVUQUZBTUlMWQ=="
    }
}
```

### Sample EMR application configration for AWS MSK:
```
[
    {
        "classification": "hbase-site",
        "properties": {
            "hbase.replication.sink-factory-class": "com.amazonaws.hbase.datasink.KafkaDataSinkImpl",
            "hbase.replication.compression-enabled": "false",
            "hbase.replication.kafka.msk.arn": "arn:aws:kafka:REGION:ACCOUNTID:cluster/CLUSTERNAME/UNIQID",
            "hbase.replication.kafka.topic.create": "true",
            "hbase.replication.bulkload.enabled": "true",
            "hbase.replication.kafka.topic-table-map": "tablename:hbase-replication",
            "hbase.replication.cluster.id": "hbase1",
            "hbase.replication.kafka.security.protocol": "PLAINTEXT"
        },
        "configurations": []
    },
    {
        "classification": "hbase-env",
        "properties": {},
        "configurations": [
            {
                "classification": "export",
                "properties": {
                    "HBASE_CLASSPATH": "$HBASE_CLASSPATH:/usr/lib/hbase-extra/kafka-sink-alpha-0.1.jar"
                },
                "configurations": []
            }
        ]
    }
]
```

### Sample EMR application configration for AWS Kinesis Streaming:
```
[
    {
        "classification": "hbase-site",
        "properties": {
            "hbase.replication.kinesis.aggregation-enabled": "false",
            "hbase.replication.sink-factory-class": "com.amazonaws.hbase.datasink.KinesisDataSinkImpl",
            "hbase.replication.compression-enabled": "false",
            "hbase.replication.kinesis.stream-table-map": "tablename:hbase-replication",
            "hbase.replication.bulkload.enabled": "true",
            "hbase.replication.kinesis.region": "REGION",
            "hbase.replication.cluster.id": "hbase1"
        },
        "configurations": []
    },
    {
        "classification": "hbase-env",
        "properties": {},
        "configurations": [
            {
                "classification": "export",
                "properties": {
                    "HBASE_CLASSPATH": "$HBASE_CLASSPATH:/usr/lib/hbase-extra/kinesis-sink-alpha-0.1.jar"
                },
                "configurations": []
            }
        ]
    }
]
``` 
## References
* https://issues.apache.org/jira/browse/HBASE-11367
* https://issues.apache.org/jira/browse/HBASE-11992
* https://issues.apache.org/jira/browse/HBASE-12254
* https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html
* https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-config.html
* https://github.com/awslabs/kinesis-aggregation

