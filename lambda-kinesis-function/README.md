This module is a sample AWS lambda function to consume Kinesis records, replicated from a from HBase and produced by the 'sensorsimulator' module. 
The records will be enriched and written to the same source table, in a separate column family out of HBase replication.
