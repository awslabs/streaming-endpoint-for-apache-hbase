// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.hbase.DataSink;

public class KafkaDataSinkImpl extends DataSink {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaDataSinkImpl.class);
	private KafkaConfigurationUtil configUtil;
	private Producer producer;
	private long sentMessageCount = 0L;

	public KafkaDataSinkImpl(Configuration config) {
		super(config);
	}

	public List<Entry> filter(final List<Entry> oldEntries) {
		
		List<Entry> entries = new ArrayList<>();
		for (Entry e : oldEntries) {
			TableName tableName = e.getKey().getTableName();

			if ( this.getConfigurationUtil().getTopicFromTableName(tableName.getNameAsString()) != null ) {
				entries.add(e);
			} else {
				LOG.debug("Not mapped to stream: " + tableName.toString());
			}
		}

		return entries;
	}
	
	public synchronized void flush() {
		producer.flush();
	}
	
	private KafkaConfigurationUtil getConfigurationUtil() {
		if ( this.configUtil == null) {
			this.configUtil = new KafkaConfigurationUtil(this.getConfig());	
		}
		
		return this.configUtil;
	}

	@Override
	public boolean isBlocking() {
		return true;
	}

	@Override
	public void putRecord(ByteBuffer buffer, String tablename) throws Exception {
			this.producer = KafkaProducerFactory.getProducer(this.getConfigurationUtil().getConfigurationProperties());
			long time = System.currentTimeMillis();
			Long index = time + sentMessageCount++;
			final ProducerRecord<String, ByteBuffer> record =  new ProducerRecord<String, ByteBuffer>(
					this.getConfigurationUtil().getTopicFromTableName(tablename), 
					new Long(index).toString(), 
					buffer);
			try {
				RecordMetadata metadata = (RecordMetadata) producer.send(record).get(); // This will block
				long elapsedTime = System.currentTimeMillis() - time;
				LOG.debug(
						"sent record(key=%s ) " +  "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), 
						record.value(), 
						metadata.partition(),
						metadata.offset(), 
						elapsedTime);
			} catch (ExecutionException e) {
				LOG.error("Error in sending record" ,e);
				if (e.getCause().getClass() == TimeoutException.class) {
					LOG.error("Seems {} does not exist. checking ..", record.topic());
					Properties config = new Properties();
					config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.configUtil.getBootstrapServers());
					KafkaAdminUtil adminUtil = new KafkaAdminUtil(config); 
					if (adminUtil.listTopics().contains(record.topic()) == false) {
						if (this.configUtil.createTopicIfNotFound() == true) {
							adminUtil.createTopic(record.topic(),
										this.configUtil.getTopicPartitions(), 
										this.configUtil.getTopicReplicationFactor());
						}
					}
				}
			}
	}

	@Override
	public void putRecord(ByteBuffer buffer, String tablename, String parition) throws Exception {
		throw new RuntimeException("Not implemented!");
		
	}

	@Override
	public boolean supportsTransaction() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void beginTransaction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitTransaction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abortTransaction() {
		// TODO Auto-generated method stub
		
	}
}
