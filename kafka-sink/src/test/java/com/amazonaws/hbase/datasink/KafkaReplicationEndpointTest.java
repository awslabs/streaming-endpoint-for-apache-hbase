// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazonaws.hbase.model.HBaseWALEntry;
import com.amazonaws.hbase.StreamingReplicationEndpoint;
import com.amazonaws.hbase.model.HBaseCell;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;


import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.TemporaryFolder;

public class KafkaReplicationEndpointTest {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaReplicationEndpointTest.class);
	private HBaseTestingUtility UTIL;

	private static final String PEER_NAME = "kfkareplicationendpoint";
	private static final String TOPIC_NAME = "topic";
	private static final TableName TABLE_NAME = TableName.valueOf("table");
	private static final String ROWKEY = "row";
	private static final byte[] ROWKEY_BYTES = Bytes.toBytes(ROWKEY);
	private static final String COLUMN_FAMILY = "columnfamily";
	private static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
	private static final String QUANTIFIER = "quantifier";
	private static final byte[] QUANTIFIER_BYTES = Bytes.toBytes(QUANTIFIER);
	private static final String VALUE = "value";
	private static final byte[] VALUE_BYTES = Bytes.toBytes(VALUE);

	private int numRegionServers;

	private TableDescriptor table;
	private Connection connection;

	private int port = 9092;

	private Properties effectiveConfig;
	private File logDir;
	private final TemporaryFolder tmpFolder = new TemporaryFolder();
	private KafkaServer kafka;

	private KafkaConfigurationUtil configUtil;

	/**
	 * This will configure a mini embedded Hbase test cluster and a single node in memory Kafka cluster for integration testing. 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {

		Configuration config = HBaseConfiguration.create();

		config.set("hbase.replication.kafka.topic-table-map", TABLE_NAME.getNameAsString() + ":" + TOPIC_NAME);
		config.set("hbase.replication.kafka.bootstrap.servers", "localhost:" + new Integer(port).toString());
		config.set("hbase.replication.kafka.batch.size", "1");
		config.setBoolean("hbase.replication.bulkload.enabled", true);
		config.set("hbase.replication.kafka.request.timeout.ms", "100");
		config.set("hbase.replication.kafka.retries", "3");
		config.set("hbase.replication.kafka.retry.backoff.ms", "1000");
		config.set("hbase.replication.kafka.transaction.timeout.ms", "1000");
		config.set("hbase.replication.sink-factory-class", "com.amazonaws.hbase.datasink.KafkaDataSinkImpl");
		config.setBoolean("hbase.replication.compression-enabled", false);
		config.set("hbase.replication.kafka.security.protocol", "PLAINTEXT");

		config.set("hbase.replication.cluster.id", "hbase1");

		configUtil = new KafkaConfigurationUtil(config);

		UTIL = new HBaseTestingUtility(config);
		UTIL.startMiniCluster();
		
		numRegionServers = UTIL.getHBaseCluster().getRegionServerThreads().size();

		connection = ConnectionFactory.createConnection(UTIL.getConfiguration());

		tmpFolder.create();
		logDir = tmpFolder.newFolder("logs");
		effectiveConfig = effectiveConfigFrom(configUtil.getConfigurationProperties());

		final boolean loggingEnabled = true;

		final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);

		kafka = TestUtils.createServer(kafkaConfig, Time.SYSTEM);
		LOG.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
		        brokerList(), zookeeperConnect());
		
		//this.createTopic(configUtil.getTopicFromTableName( TABLE_NAME.getNameAsString()));
		
	}

	/**
	 * This will add few kafka configuration, in addition to Context configuration.
	 * @param initialConfig 	A properties that will be overrider.
	 * @return
	 * @throws IOException
	 */
	private Properties effectiveConfigFrom(final Properties initialConfig) throws IOException {
		final Properties effectiveConfig = new Properties();
		effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
		effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
		effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
		effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
		effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);
		effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeperConnect());
		effectiveConfig.put(KafkaConfig$.MODULE$.DefaultReplicationFactorProp(), 1);
		effectiveConfig.put("offsets.topic.replication.factor", "1");
		effectiveConfig.putAll(initialConfig);
		effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
		return effectiveConfig;
	}

	/**
	 * zookeeper.connect property.
	 * @return
	 */
	public String zookeeperConnect() {
		MiniZooKeeperCluster zk = UTIL.getZkCluster();
		
		String HOST = "localhost";
		int port = zk.getClientPort();
		
		return HOST + ":" + Integer.toString(port);
	}

	/**
	 * creates a Streaming Replication peer in Hbase.
	 * @return
	 * @throws IOException
	 */
	private boolean createReplictionPeer() throws IOException {
		Admin admin = UTIL.getAdmin();
		
		try {
			admin.addReplicationPeer(PEER_NAME, new ReplicationPeerConfig()
					.setReplicationEndpointImpl("com.amazonaws.hbase.StreamingReplicationEndpoint"), true);
		} catch (Exception e) {
			LOG.error("Unable to add the replication peer. ", e);
			return false;
		}
		LOG.info("--- Added Replication Peer " + StreamingReplicationEndpoint.class.getCanonicalName() + "---");
		return true;
	}

	/**
	 * Creates  table in HBase to replicate.
	 * @return
	 * @throws IOException
	 */
	private boolean createTable() throws IOException {
		try {
			table = TableDescriptorBuilder
					.newBuilder(TABLE_NAME).setColumnFamily(ColumnFamilyDescriptorBuilder
							.newBuilder(COLUMN_FAMILY_BYTES).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
					.build();

			Admin admin = UTIL.getAdmin();
			admin.createTable(table);
			UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
			LOG.info("--- created test table ---");
		} catch (Exception e) {
			LOG.error("Failed to create HBase table: " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Puts 1 test record in HBase table to replicate
	 * @return
	 * @throws Exception
	 */
	private boolean putOneRecord() throws Exception {
		Put put = new Put(ROWKEY_BYTES);
		Table table = connection.getTable(TABLE_NAME);

		put.addColumn(COLUMN_FAMILY_BYTES, COLUMN_FAMILY_BYTES, VALUE_BYTES);

		table.put(put);
		LOG.info("--- Put one record into the table for replication ---");
		Thread.sleep(10000);
		return true;
	}

	/**
	 * End-to-end Hbase streaming replication test.
	 * @throws Exception
	 */
	@Test
	public void testKafkaReplicationEndpoint() throws Exception {
		createTable();
		if (createReplictionPeer() == false) {
			Assert.fail("Unable to add replication peer.");
		}
		putOneRecord();
		LOG.info("--- Replicated records ---");
		Thread.sleep(3000);
		String jsonData = readRecrod();
		LOG.info("Serialized HBase Put item: " + jsonData);
	
		try {
			HBaseWALEntry entry  = (HBaseWALEntry)new ObjectMapper().readValue(jsonData, HBaseWALEntry.class);

			assertEquals("Checking the tablename ",TABLE_NAME.toString(),new String(entry.getWalKey().getTableName()));
			
			//Check families
			Set<String> families = entry.getWalEdit().getFamilies();					
			// We are expecting only 1 columnFamily.
			assertEquals("Checking ColumnFamily count",families.size(),1);
			
			HBaseCell cell = (HBaseCell) entry.getWalEdit().getCells().get(0);
			// check attributes to be as expected
			assertEquals("Checking ColumnFamily",new String(cell.getFamily()), COLUMN_FAMILY);
			assertEquals("Checking rowKey",new String(cell.getRow()),ROWKEY);
			assertEquals("Checking action",cell.getType().toLowerCase(),"put");
			assertEquals("Checking value",new String(cell.getValue()),VALUE);
			
		} catch (JsonMappingException e) {
			e.printStackTrace();
			Assert.fail("Unable to deserialize");
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			Assert.fail("Unable to deserialize.");
		} 

	}

	private String readRecrod() {
		Properties consumProperties = configUtil.getConfigurationProperties();
		consumProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
		consumProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		consumProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<String, ByteBuffer> consumer =
			    new KafkaConsumer<String, ByteBuffer>(consumProperties);
		
		consumer.subscribe(Collections.singletonList(configUtil.getTopicFromTableName(TABLE_NAME.getNameAsString()))); 
		ByteBuffer res = ByteBuffer.wrap("".getBytes());
		int count=0;
		try {	
			while ( count == 0) {
		    	ConsumerRecords<String, ByteBuffer> records = consumer.poll(1000);
		    	for (ConsumerRecord<String, ByteBuffer> record : records) 
		        {
		            LOG.debug("topic = %s, partition = %d, offset = %d," +
			                "customer = %s, country = %s\n",
			                record.topic(), record.partition(), record.offset(),
			                record.key(), record.value());
		            
		            res = record.value();
		            count++;
		        }
			}
		    assertEquals(1,count);
		} finally {
		    consumer.close(); 
		}
		
		return StandardCharsets.UTF_8.decode(res).toString();
	}

	/**
	 * Creates a topic with 1 patition in Kafka
	 * @param topic
	 */
	public void createTopic(final String topic) {
		createTopic(topic, 1, (short) 1, Collections.emptyMap());
	}

	/**
	 * Creates a topic with partitions and replication.
	 * @param topic
	 * @param partitions
	 * @param replication
	 */
	public void createTopic(final String topic, final int partitions, final short replication) {
		createTopic(topic, partitions, replication, Collections.emptyMap());
	}

	/**
	 * Create topic with topic options
	 * @param topic
	 * @param partitions
	 * @param replication
	 * @param topicConfig
	 */
	public void createTopic(final String topic, final int partitions, final short replication,
			final Map<String, String> topicConfig) {
		LOG.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }", topic, partitions,
				replication, topicConfig);

		final Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

		try (final AdminClient adminClient = AdminClient.create(properties)) {
			final NewTopic newTopic = new NewTopic(topic, partitions, replication);
			newTopic.configs(topicConfig);
			adminClient.createTopics(Collections.singleton(newTopic)).all().get();
		} catch (final InterruptedException | ExecutionException fatal) {
			throw new RuntimeException(fatal);
		}

	}

	/**
	 * Delete Topic
	 * @param topic
	 */
	public void deleteTopic(final String topic) {
		LOG.debug("Deleting topic {}", topic);
		final Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

		try (final AdminClient adminClient = AdminClient.create(properties)) {
			adminClient.deleteTopics(Collections.singleton(topic)).all().get();
		} catch (final InterruptedException e) {
			throw new RuntimeException(e);
		} catch (final ExecutionException e) {
			if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Uncompress byte from Gzip
	 * 
	 * @param bytes bytes to uncompress
	 * @return returns bytes
	 * @throws IOException
	 */
	private byte[] gzipUncompress(byte[] bytes) throws IOException {
		ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
		GZIPInputStream zipStream = new GZIPInputStream(byteStream);
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		byte[] buffer = new byte[1024];
		int len;
		while ((len = zipStream.read(buffer)) > 0) {
			out.write(buffer, 0, len);
		}
		zipStream.close();
		out.close();

		return out.toByteArray();
	}

	/**
	 * Stop Hbase Cluster and Stop the Kafka cluster.
	 * @throws Exception
	 */
	@After
	public void stopServer() throws Exception {
		this.stop();
		UTIL.shutdownMiniCluster();
		
	}

	/**
	 * Get broker list
	 * @return
	 */
	public String brokerList() {
		return String.join(":", kafka.config().hostName(),
				Integer.toString(kafka.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))));
	}

	/**
	 * Stop the Kafka gracefully
	 */
	public void stop() {
		LOG.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...", brokerList(),
				zookeeperConnect());
		kafka.shutdown();
		kafka.awaitShutdown();
		LOG.debug("Removing temp folder {} with logs.dir at {} ...", tmpFolder, logDir);
		tmpFolder.delete();
		LOG.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...", brokerList(),
				zookeeperConnect());
	}
}
