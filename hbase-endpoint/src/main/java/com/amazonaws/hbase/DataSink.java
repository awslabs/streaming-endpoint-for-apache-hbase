// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.wal.WAL;

public abstract class DataSink {
	/**
	 * A Hadoop configuration instance
	 */
	private Configuration config;
	
	/**
	 * Constructor with a Hadoop Configuration
	 * @param config
	 */
	public  DataSink(Configuration config) {
		this.config = config;
	}
	
	/**
	 * Is data sink block per putRecord or it's asynchronous.
	 * @return 
	 */
	public abstract boolean isBlocking();
	
	/**
	 * This method will push a record to the stream.
	 * @param buffer The data to be pushed into the stream
	 * @param tablename The HBase table name this data belongs to.
	 * @throws Exception if the data push failed and exception is raised.
	 */
	public abstract void putRecord(ByteBuffer buffer,String tablename) throws Exception;
	
	/**
	 * This method will push a record to the stream.
	 * @param buffer The data to be pushed into the stream
	 * @param tablename The HBase table name this data belongs to.
	 * @param partition A string to partition records inside stream
	 * @throws Exception if the data push failed and exception is raised.
	 */
	public abstract void putRecord(ByteBuffer buffer,String tablename,String parition) throws Exception;
	
	/**
	 * block on flush for any remaining record in to be push into the stream.
	 */
	public abstract void flush();
	
	/**
	 * Any custom filter by the Data sink. This is to allow the replication to know which records can not be replicated before we pass them to the data sink.
	 * For example, restriction on records size, unregistered tables, etc. 
	 * @param filter
	 * @return a like of Entries that have passed the filter.
	 */
	public abstract List<WAL.Entry> filter(List<WAL.Entry> filter);

	/**
	 * Returns an instance of configuration.
	 * @return
	 */
	public Configuration getConfig() {
		return config;
	}
	
	/**
	 * Does datasink support transaction ?
	 * @return
	 */
	public abstract boolean supportsTransaction(); 
	
	/**
	 * This starts a transaction when supported
	 */
	public abstract void beginTransaction();
	
	/**
	 * Commit
	 */
	public abstract void commitTransaction();
	
	/**
	 * Abort
	 */
	public abstract void abortTransaction();
	
}
