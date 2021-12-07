// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.hbase.ConfigurationUtil;
import com.amazonaws.regions.Regions;

public class FirehoseConfigurationUtil extends ConfigurationUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(FirehoseConfigurationUtil.class);
	
	private HashMap<String, String> kFirehoseTableMap = null;

	private ClientConfiguration clientConfig;
	
	public static final String REPLICATIOM_FIREHOSE = 
			ConfigurationUtil.BASE_HBASE + ".firehose";
	
	public static final String REPLICATION_KINESIS_FIREHOSE_TABLE_MAP = 
			REPLICATIOM_FIREHOSE+".stream-table-map";
	
	public static final String TABLE_MAP_DELIMITER = ":";
	
	public static final String FIREHOSE_REGION = 
			REPLICATIOM_FIREHOSE+".region";
	
	public static final String FIREHOSE_ENDPOINT = 
			REPLICATIOM_FIREHOSE+".endpoint";
	
	public static final String FIREHOSE_ENDPOINT_PORT = 
			REPLICATIOM_FIREHOSE+".endpoint-port";
	
	public static final String BATCHED = 
			REPLICATIOM_FIREHOSE+".batch.enabled";
	
	public static final String STREAM_REQUEST_TIMEOUT = 
			REPLICATIOM_FIREHOSE+".request-timeout";
	
	
	
	
	public FirehoseConfigurationUtil(Configuration conf) {
		super(conf);
		this.kFirehoseTableMap = new HashMap<String,String>();
		String[] mappingList = conf.getStrings(REPLICATION_KINESIS_FIREHOSE_TABLE_MAP,new String[] {});
		
		for (String s : mappingList ) {
			String[] entry = s.split(TABLE_MAP_DELIMITER);
			kFirehoseTableMap.put(entry[0],entry[1]);
		}
		
		LOG.debug("Firehose to table map: " + kFirehoseTableMap.toString() );
	}
	
	protected ClientConfiguration getClientConfiguration() {
		this.clientConfig = new ClientConfiguration();

		return this.clientConfig;
	}
	
	public String getFirehoseRegion() {
		String streamRegion = this.conf.get(FIREHOSE_REGION);
		
		try {
			if (streamRegion == null ) {
				LOG.info("the "+ FIREHOSE_REGION + "is not set, we are going to rely on EC2 metadata.");
				streamRegion = Regions.getCurrentRegion().getName();
			}
		} catch (SdkClientException e) {
			LOG.error("Kinesis Firehose Endpoint is not running on EC2 so we couldn't rely on "+ FIREHOSE_REGION +" to assume the region.");
			throw e;
		}
		
		return this.conf.get(FIREHOSE_REGION,streamRegion);
	}
	
	public String getFirehoseNameFromTableName(String tname ) {
		return kFirehoseTableMap.get(tname);
	}

	public boolean isBatchPutsEnabled() {
		return this.conf.getBoolean(BATCHED, false);
	}
	
}
