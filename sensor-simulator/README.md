This module is a sample HBase client to generate replication traffic for demonstration purpose and only takes the table name as a commandline argument.
This module runs as a java application and is launched as an EMR step on a cluster with the following bash command:

```
hadoop jar /var/lib/aws/emr/step-runner/hadoop-jars/command-runner.jar bash -c java -cp /usr/lib/hbase-extra/sensor-simulator-alpha-0.1.jar:`hbase classpath` com.amazonaws.examples.sensorsimulator.Main tablename'
```

The client:
  * Creates a table if it doesn't exist at start
  * Sets up the replaication endpoint "StreamingReplication" with customer custom implementation "com.amazonaws.hbase.StreamingReplicationEndpoint".
  * Continuesly writes into the HBase table using org.apache.hadoop.hbase.client.Table.put
  
