// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.hbase.DataSink;
import com.amazonaws.hbase.UUIDHelper;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

public class KinesisDataSinkImpl extends DataSink {
	private MessageDigest md;
	private static final Logger LOG = LoggerFactory.getLogger(KinesisDataSinkImpl.class);
	
	private boolean isBlocking = true;
	private KinesisProducer kinesis = null;

	private KinesisConfigurationUtil configUtil;
	
	/**
	 * Constructor
	 * @param config
	 */
	public KinesisDataSinkImpl(Configuration config)  {
		super(config);
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			//This should never happen as of java 7.
			LOG.error(" Every implementation of the Java platform is required to support the following standard MessageDigest algorithms:\n"
					+ "\n"
					+ "    MD5\n"
					+ "    SHA-1\n"
					+ "    SHA-256\n"
					+ "", e);
			e.printStackTrace();
		}
	}
	
	/**
	 * This data sunk blocks per putRecord
	 */
	public boolean isBlocking() {
		return this.isBlocking;
	}

	/**
	 * putRecord implementation. The records will be partitioned in shards 
	 * based on a random generated MD5 hash. We need table name to know which 
	 * stream the record belongs to.
	 * @param buffer
	 * @param tablename
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void putRecord(ByteBuffer buffer, String tablename) throws IOException, InterruptedException, ExecutionException {
		String partition = UUIDHelper.getBase64UUID();
		LOG.debug("Putting record in random partition: " + partition);
		this.putRecord( buffer,  tablename,partition);
	}
	
	/**
	 * putRecord implementation. The records are partitioned based on MD5
	 * hash generated on the partition parameter. We need tablename to know which stream the records belongs to. 
	 * @param buffer
	 * @param tablename
	 * @param partition
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void putRecord(ByteBuffer buffer, String tablename,String partition) throws IOException, InterruptedException, ExecutionException {
		if (kinesis == null) { // creating the producer when there is a request.
			KinesisProducerConfiguration config = this.getConfigurationUtil().getKPLConfiguration();
			LOG.debug("First Time ptoducer. endpoint " + config.getKinesisEndpoint() + " port: " + config.getKinesisPort() );
			this.kinesis = KinesisProducerFactory.getProducer(config);
		}
		
		
		md.update(partition.getBytes());
		String digest = Base64.getEncoder().encodeToString(md.digest());
		LOG.debug("Putting record in digest partition: " + digest);
		String destination = this.getConfigurationUtil().getStreamNameFromTableName(tablename);
		Future<UserRecordResult> putFuture = (Future<UserRecordResult>) kinesis.addUserRecord(destination, digest, buffer);
		long time = System.currentTimeMillis();
		LOG.debug("Starting a put " + time);
		UserRecordResult result = putFuture.get(); // this does block     
		LOG.debug("Out of Put " + System.currentTimeMillis());
		
		if (result.isSuccessful()) {         
			LOG.debug(
					"Put record into shard= {} PartitionKey = {}, time={} "
					, result.getShardId()
					, digest
					, System.currentTimeMillis() - time); 
		} else {
			for (Attempt attempt : result.getAttempts()) {
				LOG.error(attempt.getErrorMessage());
				throw new IOException("Record faild to replicate");
			}
		}		
	}

	/**
	 * get the configuration
	 */
	public Configuration getConfig() {
		return super.getConfig();
	}

	/**
	 * Block until records are flushed by KPL.
	 */
	public synchronized void flush() {
		kinesis.flushSync();
	}
	
	/**
	 * get instance of configurationUtil. This will initialize configurationUtil if it isn't already.
	 * @return
	 */
	private KinesisConfigurationUtil getConfigurationUtil() {
		if ( this.configUtil == null) {
			this.configUtil = new KinesisConfigurationUtil(this.getConfig());	
		}
		
		return this.configUtil;
	}
	
	/**
	 * We filter the Entries that are not belonged to a mapped table to stream
	 */
	public List<Entry> filter(final List<Entry> oldEntries) {
		
		List<Entry> entries = new ArrayList<>();
		for (Entry e : oldEntries) {
			TableName tableName = e.getKey().getTableName();

			if ( this.getConfigurationUtil().getStreamNameFromTableName(tableName.getNameAsString()) != null ) {
				entries.add(e);
			} else {
				LOG.debug("Not mapped to stream: " + tableName.toString());
			}
		}

		return entries;
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
