// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.hbase.ConfigurationUtil;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClient;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersResult;

public class KafkaConfigurationUtil extends ConfigurationUtil {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConfigurationUtil.class);
	
	private HashMap<String, String> kStreamTableMap = null;
	
	public static final String REPLICATION_KAFKA = 
			ConfigurationUtil.BASE_HBASE + ".kafka";
	
	public static final String REPLICATION_KAFKA_TOPIC_TABLE_MAP = 
			REPLICATION_KAFKA+".topic-table-map";
	
	public static final String TABLE_MAP_DELIMITER = ":";
	
	public static final String KAFKA_BOOTSTRAP_SERVERS = 
			REPLICATION_KAFKA+".bootstrap.servers";
	
	public static final String MSK_ARN = 
			REPLICATION_KAFKA+".msk.arn";
	
	public static final String KAFKA_BATCH_SIZE_CONFIG = 
			REPLICATION_KAFKA+".batch.size";
	
	public static final String KAFKA_REQUEST_TIMEOUT_MS_CONFIG = 
			REPLICATION_KAFKA+".request.timeout.ms";
	
	public static final String KAFKA_RETRIES_CONFIG =
			REPLICATION_KAFKA +".retries";
	
	public static final String KAFKA_RETRY_BACKOFF_MS_CONFIG =
			REPLICATION_KAFKA +".retry.backoff.ms";
	
	public static final String KAFKA_TRANSACTION_TIMEOUT_CONFIG = 
			REPLICATION_KAFKA +".transaction.timeout.ms";
	
	public static final String KAFKA_SECURITY_PROTOCOL = 
			REPLICATION_KAFKA +".security.protocol";
	
	public static final String KAFKA_SSL_TRUSTSTORE_LOCATION = 
			REPLICATION_KAFKA +".ssl.truststore.location";
	
	public static final String KAFKA_SSL_TRUSTSTOR_PASSWORD = 
			REPLICATION_KAFKA + ".ssl.truststore.password";
	
	public static final String KAFKA_SSL_KEYSTORE_LOCATION = 
			REPLICATION_KAFKA +".ssl.keystore.location";
	
	public static final String KAFKA_SSL_KEYSTORE_PASSWORD = 
			REPLICATION_KAFKA +".ssl.keystore.location";
	
	public static final String KAFKA_SSL_KEY_PASSWORD = 
			REPLICATION_KAFKA +".ssl.key.password";
	
	public static final String KAFKA_TOPIC_CREATE = 
			REPLICATION_KAFKA +".topic.create";
	
	public static final String KAFKA_TOPIC_PARTITIONS = 
			REPLICATION_KAFKA +".topic.partitions";
	
	public static final String KAFKA_TOPIC_REPLICATION = 
			REPLICATION_KAFKA +".topic.replication-factor";
	
	private static String bootstraps=null;
	/**
	 * Constructor
	 * @param conf
	 */
	public KafkaConfigurationUtil(Configuration conf) {
		super(conf);
		this.kStreamTableMap = new HashMap<String,String>();
		String[] mappingList = conf.getStrings(REPLICATION_KAFKA_TOPIC_TABLE_MAP,new String[] {});
		for (String s :mappingList) {
			String[] entry = s.split(TABLE_MAP_DELIMITER);
			kStreamTableMap.put(entry[0],entry[1]);
		}
		
		LOG.info("Stream to table map: " + kStreamTableMap.toString() );
	}
	
	
	/**
	 * Get bootstrap.servers
	 * Default: null
	 * @return
	 */
	public String getBootstrapServers() {
		if (this.conf.get(KAFKA_BOOTSTRAP_SERVERS) != null) {
			KafkaConfigurationUtil.bootstraps = this.conf.get(KAFKA_BOOTSTRAP_SERVERS);
			return this.conf.get(KAFKA_BOOTSTRAP_SERVERS);
		} else 
		if (KafkaConfigurationUtil.bootstraps == null) {
			if ( this.getMskArn() != null ) {
				GetBootstrapBrokersRequest request = new GetBootstrapBrokersRequest().withClusterArn(this.getMskArn());
				AWSKafka mskClient =  AWSKafkaClient.builder().standard().build();
				GetBootstrapBrokersResult result = mskClient.getBootstrapBrokers(request);
				KafkaConfigurationUtil.bootstraps = result.getBootstrapBrokerString();
			}
		}
		return KafkaConfigurationUtil.bootstraps;
	}
	
	public String getMskArn() {
		return this.conf.get(MSK_ARN);
	}
	/**
	 * Get transaction.timeout.ms 
	 * Default: 60000
	 * @return
	 */
	public String getTransactionTimeout() {
		return this.conf.get(KAFKA_TRANSACTION_TIMEOUT_CONFIG,"60000");
	}
	
	/**
	 * Get retry.backoff.ms
	 * Default: 100
	 * @return
	 */
	public String getRetryBackoff() {
		return this.conf.get(KAFKA_RETRY_BACKOFF_MS_CONFIG,"100");
	}

	/**
	 * Get retries
	 * Default: 30000
	 * @return
	 */
	public String getRetries() {
		return this.conf.get(KAFKA_RETRIES_CONFIG,"30000");
	}
	
	/**
	 * Get request.timeout.ms
	 * Default: 120000
	 * @return
	 */
	public String getRequestTimeout() {
		return this.conf.get(KAFKA_REQUEST_TIMEOUT_MS_CONFIG,"120000");
	}
	
	/**
	 * Get batch.size
	 * Default: 16384
	 * @return
	 */
	public String getBatchSize() {
		return this.conf.get(KAFKA_BATCH_SIZE_CONFIG,"16384");
	}
	
	/**
	 * get mapped stream from table name
	 * @param tname
	 * @return
	 */
	public String getTopicFromTableName(String tname ) {
		return kStreamTableMap.get(tname);
	}
	
	/**
	 * security.protocol
	 * @return
	 */
	public String getSecurityProtocol() {
		return this.conf.get(KAFKA_SECURITY_PROTOCOL,"PLAINTEXT");
	}
	
	/**
	 * ssl.truststore.location
	 * @return
	 */
	
	public String getSecuritySSLTrustStoreLocation() {
		return this.conf.get(KAFKA_SSL_TRUSTSTORE_LOCATION,null);
	}
	
	/**
	 * ssl.truststore.password
	 * @return
	 */
	public String getSecuritySSLTrustStorePassword() {
		return this.conf.get(KAFKA_SSL_TRUSTSTOR_PASSWORD,null);
	}
	
	/**
	 * ssl.keystore.location
	 * @return
	 */
	public String getSecuritySSLKeyStoreLocation() {
		return this.conf.get(KAFKA_SSL_KEYSTORE_LOCATION,null);
	}
	
	/**
	 * ssl.keystore.password
	 * @return
	 */
	public String getSecuritySSLKeyStorePassword() {
		return this.conf.get(KAFKA_SSL_KEYSTORE_PASSWORD,null);
	}
	
	/**
	 * ssl.key.password
	 * @return
	 */
	public String getSecuritySSLKeyPassword() {
		return this.conf.get(KAFKA_SSL_KEY_PASSWORD,null);
	}
	
	/**
	 * Create topic in kafka if it doesn't exist
	 * Default: false
	 * @return
	 */
	public boolean createTopicIfNotFound() {
		return this.conf.getBoolean(KAFKA_TOPIC_CREATE, false);
	}
	
	/**
	 * If creating a topic, what should be the replication factor.
	 * Default 2
	 * @return
	 */
	public short getTopicReplicationFactor( ) {
		return Short.parseShort(this.conf.get(KAFKA_TOPIC_REPLICATION, "2"));
	}
	
	/**
	 * If creating topic, how many partitions
	 * Default: 1 and best practice is >= number of regions
	 * @return
	 */
	public Integer getTopicPartitions() {
		return this.conf.getInt(KAFKA_TOPIC_PARTITIONS, 1);
	}
	
	/**
	 * Build a properties for Kafka producer constructor from Hadoop Configuration.
	 * @return
	 */
	public Properties getConfigurationProperties() {
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, this.getBatchSize());
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, this.getTransactionTimeout());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,this.getRequestTimeout());
        props.put(ProducerConfig.RETRIES_CONFIG,this.getRetries());
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,this.getRetryBackoff());
        if ( this.getSecurityProtocol() != null ) props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,this.getSecurityProtocol());
        if ( this.getSecuritySSLKeyStoreLocation() != null ) props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.getSecuritySSLKeyStoreLocation());
        if ( this.getSecuritySSLTrustStoreLocation() != null ) props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.getSecuritySSLTrustStoreLocation());
        if ( this.getSecuritySSLKeyPassword() != null ) props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.getSecuritySSLKeyPassword());
        if ( this.getSecuritySSLTrustStorePassword() != null ) props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.getSecuritySSLTrustStorePassword());
        if ( this.getSecuritySSLKeyPassword() != null ) props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.getSecuritySSLKeyPassword());
        
		return props;
		
	}
	

}
