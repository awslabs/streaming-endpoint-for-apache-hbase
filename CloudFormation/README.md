A sample Cloudformation template and required material to launch a replication demonstration. The template will launch:
  1. A VPC. 
  2. A public subnet. 
  3. 2 private subnets.
  4. InternetGateway and attachment.
  5. 2 NAT gateways, 2 EIPs and attachments to the NAT Gateways.
  6. A Route table for the public subnet, the route and route assignment.
  7. Private routes and associations for the private subnets.
  8. A Lambda function "ScriptSetupLambdaFunction" to download consumer application jars into a Writable S3 location, from Github repository. 
['bootstrap-action.sh','lambda-kinesis-function-alpha-0.1.jar','lambda-kafka-function-alpha-0.1.jar']
  9. An EMR cluster with Apache HBase.
  10. Custom security groups and rules for EMR instances.
  11. A consumer lambda AWS function. "LambdaKinesisConsumer" or "LambdaKafkaConsumer".
  12. A Kinesis or Kafka stream as replication destination.
  13. A Kafka or Kinesis event sour mapping for the Lambda consumer. "KafkaEventSourceMapping" or "KinesisEventSourceMapping"
  14. IAM roles to launch the EMR cluster "EMRClusterServiceRole", IAM ec2 roles for EMR instance to allow access to S3, KAfka or Kinesis "EMRClusterinstanceProfileRole" and assignement to EC2 Instance profile "EMRClusterinstanceProfile". 
  15. A Lambda funtion for parsing S3 URL "TransformFunction".
  16. A Lambda execution role for consumer lambda functions, allowing access to Kinesis or Kafka "AppLambdaExecutionRole".  
  17. A sample HBase client application, as an EMR step to produce records.
  18. A replication peer to stream WALEdits into a stream as defined per HBase peer configuration.

