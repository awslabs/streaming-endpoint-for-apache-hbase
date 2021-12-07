// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import java.nio.charset.StandardCharsets;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.amazonaws.hbase.Constants;
import com.amazonaws.hbase.DataSink;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.services.kinesisfirehose.model.InvalidKMSResourceException;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;

//TODO:

public class FirehoseDataSinkImpl extends DataSink {
	
	private static final Logger LOG = LoggerFactory.getLogger(FirehoseDataSinkImpl.class);
	
	private boolean isBlocking = true;

	private AmazonKinesisFirehose firehose = null;
	private FirehoseConfigurationUtil configUtil;
	
	private Map<String,ConcurrentLinkedQueue<Record>> streamListMap = Collections.synchronizedMap(new HashMap<String,ConcurrentLinkedQueue<Record>>());
	private Map<String,Long> flushStats = Collections.synchronizedMap(new HashMap<String,Long>());

	/**
	 * Constructor
	 * @param config
	 */
	public FirehoseDataSinkImpl(Configuration config)  {
		super(config);
		this.configUtil = this.getConfigurationUtil();
		
		this.firehose = FirehoseProducerFactory.getProducer(config);
		
		Thread flusher = new Thread(){
		
		public void run(){
		    	try {
		    		flush();
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					LOG.info("Firehose preiodic flusher died.");
					e.printStackTrace();
				}
		    }
		};

		flusher.start();
	}
	
	/**
	 * This data sunk blocks per putRecord
	 */
	public boolean isBlocking() {
		return this.isBlocking;
	}

	/**
	 * putRecord implementation.  

	 * @param buffer
	 * @param tablename
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws InvalidRecordException 
	 */
	public void putRecord(final ByteBuffer buffer, String tablename) throws IOException, InterruptedException, ExecutionException, InvalidRecordException {

		Record record = (new Record()).withData(buffer);

		if ( this.getRecordSize(record) > Constants.MAX_RECORD_SIZE_BYTES ) {
			throw new InvalidRecordException("Record size more than 1000KB : " + this.getRecordSize(record) + ", " + record.toString());
		}
		
		String streamName = configUtil.getFirehoseNameFromTableName(tablename);
		
		if (configUtil.isBatchPutsEnabled() == false) {
			long startTime = System.currentTimeMillis();
			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setDeliveryStreamName(streamName);
			putRecordRequest.setRecord(record);
			PutRecordResult result = firehose.putRecord(putRecordRequest);
			if (LOG.isDebugEnabled()) {
				LOG.debug("PutRecord took " + Long.toString(System.currentTimeMillis() - startTime) +"ms" +
						", result: " + result.getRecordId() + 
						", recordsize: " + Integer.toString(getRecordSize(record)));
			}
		} else {
		
			if (streamListMap.containsKey(streamName) == true) { // Do we have a queue for this stream ?
				ConcurrentLinkedQueue<Record> recordsList = (ConcurrentLinkedQueue<Record>)streamListMap.get(streamName);
				if ( recordsList.size() >= Constants.MAX_BATCHED_RECORDS) {
					flush(streamName);
				}
				recordsList.add(record);
				
			} else { // If the first time we all pushing for this stream.
				ConcurrentLinkedQueue<Record> recordsList = new ConcurrentLinkedQueue<Record>();
				recordsList.add(record);
				streamListMap.put(streamName, recordsList);
			}
		}
	}

	private synchronized void flush(String streamName) {
			long startTime = System.currentTimeMillis();
		
			ConcurrentLinkedQueue<Record> recordsList = (ConcurrentLinkedQueue<Record>)streamListMap.get(streamName);
			LinkedList<Record> tmpList = new LinkedList<Record>();
			PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
			putRecordBatchRequest.setDeliveryStreamName(streamName);
			for (int counter = 0; counter < 500; counter ++ ) { 
				try {
					tmpList.add(recordsList.element());
				} catch ( NoSuchElementException e ) {
					//Someone has emptied the buffer quicker than us.
					break;
				}
			}
			PutRecordBatchResult results = null;
			putRecordBatchRequest.setRecords(tmpList);
			try {
				results = firehose.putRecordBatch(putRecordBatchRequest);
			} catch ( ResourceNotFoundException e ) {
				LOG.error(e.getErrorMessage(),e.getCause(),e.getErrorCode());
				throw new RuntimeException(e);
			} catch ( InvalidArgumentException e) {
				LOG.error("Invalid record count > 500 or invalid record size");
				throw new RuntimeException(e);
			} catch (InvalidKMSResourceException e) {
				LOG.error("Invalid KMS Key");
				throw new RuntimeException(e);
			} catch ( ServiceUnavailableException e) {
				LOG.warn("Throttled ! retrying without delay.");
			}
			
			// We just remove the records that are successful from the list. The rest will be retried.
			int successCount = 0;
			for ( PutRecordBatchResponseEntry resultentry: results.getRequestResponses() ){
				Record item = tmpList.remove();
				if ( resultentry.getRecordId() != null) {
					successCount ++;
					recordsList.remove(item);
				} 
			}
			
			addFlushstats(streamName,System.currentTimeMillis());
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("putRecordBatch success: " +successCount+ " out of results:" +results.getRequestResponses().size() + " PutRecordBatch took {} ms ", Long.toString(System.currentTimeMillis() - startTime));
			}
			
	}

	@Override
	public void flush() {
		for (String streamNAme: streamListMap.keySet() ) {
			if (flushStats.containsKey(streamNAme)) {
				if (System.currentTimeMillis() - flushStats.get(streamNAme) > Constants.S ) {
					flush(streamNAme);
					addFlushstats(streamNAme,System.currentTimeMillis());
				}
			}
		}
	}
	
	
	
	private void addFlushstats(String streamName, long currentTimeMillis) {
		this.flushStats.put(streamName,currentTimeMillis );
	}

	/**
	 * get the configuration
	 */
	public Configuration getConfig() {
		return super.getConfig();
	}

	
	/**
	 * get instance of configurationUtil. This will initialize configurationUtil if it isn't already.
	 * @return
	 */
	private FirehoseConfigurationUtil getConfigurationUtil() {
		if ( this.configUtil == null) {
			this.configUtil = new FirehoseConfigurationUtil(this.getConfig());	
		}
		
		return this.configUtil;
	}
	
	/**
	 * We filter the Entries that are not belonging to a mapped table to stream
	 */
	public List<Entry> filter(final List<Entry> oldEntries) {
		
		List<Entry> entries = new ArrayList<>();
		for (Entry e : oldEntries) {
			TableName tableName = e.getKey().getTableName();

			if ( this.getConfigurationUtil().getFirehoseNameFromTableName(tableName.getNameAsString()) != null ) {
				entries.add(e);
			} else {
				LOG.debug("Not mapped to stream: " + tableName.toString());
			}
		}
		return entries;
	}

	@Override
	public boolean supportsTransaction() {
		return false;
	}

	@Override
	public void beginTransaction() {
	}

	@Override
	public void commitTransaction() {
	}

	@Override
	public void abortTransaction() {
	}

	
	
	private int getRecordSize(final Record record) {
		ByteBuffer tdata = record.getData().duplicate();
		return tdata.array().length;
	}

	@Override
	public void putRecord(ByteBuffer buffer, String tablename, String parition) throws Exception {
		// Not supported
		
		throw new RuntimeException("Not Supported method for Firehose");
	}

}
