// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;
import com.amazonaws.hbase.ConfigurationUtil;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.regions.Regions;

public class KinesisConfigurationUtil extends ConfigurationUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(KinesisConfigurationUtil.class);
	
	private HashMap<String, String> kStreamTableMap = null;
	private KinesisProducerConfiguration kplConfig;
	private boolean isKPLAggregationEnabled;
	
	public static final Integer DEFAULT_MAX_CONNECTIONS =5;
	public static final Long DEFAULT_STREAM_REQUEST_TIMEOUT = 60000L;
	
	public static final String REPLICATIOM_KINESIS = 
			ConfigurationUtil.BASE_HBASE + ".kinesis";
	
	public static final String REPLICATION_KINESIS_STREAM_TABLE_MAP = 
			REPLICATIOM_KINESIS+".stream-table-map";
	
	public static final String TABLE_MAP_DELIMITER = ":";
	
	public static final String STREAM_REGION = 
			REPLICATIOM_KINESIS+".region";
	
	public static final String STREAM_MAX_CONNECTION = 
			REPLICATIOM_KINESIS+".max-connection";
	
	public static final String STREAM_REQUEST_TIMEOUT = 
			REPLICATIOM_KINESIS+".request-timeout";
	
	public static final String KPL_AGGREGATION_ENABLED = 
			REPLICATIOM_KINESIS+".aggregation-enabled";
	
	public static final String KPL_ENDPOINT_ADDRESS = 
			REPLICATIOM_KINESIS+".endpoint";
	
	public static final String KPL_ENDPOINT_PORT = 
			REPLICATIOM_KINESIS+".endpoint-port";
	
	public static final String KPL_CW_ENDPOINT_PORT = 
			REPLICATIOM_KINESIS+".cw-endpoint-port";
	
	public static final String KPL_CW_ENDPOINT = 
			REPLICATIOM_KINESIS+".cw-endpoint";
	
	public static final String KPL_VERIFY_SSLCERT =
			REPLICATIOM_KINESIS+".verify-ssl-cert";
	
	public static final String KPL_METRIC_LEVEL =
			REPLICATIOM_KINESIS+".metric-level";
	
	public static final String KPL_LOG_LEVEL =
			REPLICATIOM_KINESIS+".log-level";
	
	public static final String KPL_RECORD_TTL =
			REPLICATIOM_KINESIS+".record-ttl";
	
		

	public KinesisConfigurationUtil(Configuration conf) {
		super(conf);
		this.kStreamTableMap = new HashMap<String,String>();
		String[] mappingList = conf.getStrings(REPLICATION_KINESIS_STREAM_TABLE_MAP,new String[] {});
		
		for (String s : mappingList ) {
			String[] entry = s.split(TABLE_MAP_DELIMITER);
			kStreamTableMap.put(entry[0],entry[1]);
		}
		
		LOG.debug("Stream to table map: " + kStreamTableMap.toString() );
		
		this.isKPLAggregationEnabled = 
				this.conf.getBoolean(KPL_AGGREGATION_ENABLED, false);

	}
	
	protected KinesisProducerConfiguration getKPLConfiguration() {
		this.kplConfig = new KinesisProducerConfiguration()
				.setMaxConnections(this.getStreamMaxConnection())
				.setRequestTimeout(this.getStreamRequestTimeout())
				.setAggregationEnabled(this.isKPLAggregationEnabled())
				.setRegion(this.getStreamRegion())
				.setKinesisEndpoint(this.getKinesisEndpoint())
				.setKinesisPort(this.getKinesisEndpointPort())
				.setLogLevel(this.getKplLogLevel())
				.setMetricsLevel(this.getKplMetricLevel())
				.setVerifyCertificate(this.getVerifySSLCert())
				.setCloudwatchEndpoint(this.getCloudWatchEndpoint())
				.setCloudwatchPort(this.getCloudWatchEndpointPort())
				.setRecordTtl(this.getRecordTTL());
		
		return this.kplConfig;
	}
	
	public String getStreamRegion() {
		String streamRegion = this.conf.get(STREAM_REGION);
		
		try {
			if (streamRegion == null ) {
				LOG.info("the "+ STREAM_REGION + "is not set, we are going to rely on EC2 metadata.");
				streamRegion = Regions.getCurrentRegion().getName();
			}
		} catch (SdkClientException e) {
			LOG.error("Kinesis Endpoint is not running on EC2 so we couldn't rely on "+ STREAM_REGION +" to assume the region.");
			throw e;
		}
		
		return this.conf.get(STREAM_REGION,streamRegion);
	}
	
	public Long getStreamRequestTimeout() {
		return this.conf.getLong(STREAM_REQUEST_TIMEOUT,DEFAULT_STREAM_REQUEST_TIMEOUT);
	}
	
	public Integer getStreamMaxConnection() {
		return this.conf.getInt(STREAM_MAX_CONNECTION,DEFAULT_MAX_CONNECTIONS);
	}
		
	public String getKinesisEndpoint() {
		return this.conf.get(KPL_ENDPOINT_ADDRESS,"kinesis."+this.getStreamRegion()+".amazonaws.com");
	}
	
	public Integer getKinesisEndpointPort() {
		return this.conf.getInt(KPL_ENDPOINT_PORT, 443);
	}
	
	public String getCloudWatchEndpoint() {
		return this.conf.get(KPL_CW_ENDPOINT,"monitoring."+this.getStreamRegion()+".amazonaws.com");
	}
	
	public Integer getCloudWatchEndpointPort( ) {
		return  this.conf.getInt(KPL_CW_ENDPOINT_PORT, 443);
	}
	
	public boolean getVerifySSLCert() {
		return this.conf.getBoolean(KPL_VERIFY_SSLCERT, true);
	}
	
	public String getKplMetricLevel() {
		return this.conf.get(KPL_METRIC_LEVEL,"none");
	}
	
	public String getKplLogLevel() {
		return this.conf.get(KPL_LOG_LEVEL,"info").toLowerCase(); 
	}
	

	public Integer getRecordTTL() {
		return this.conf.getInt(KPL_RECORD_TTL, Integer.MAX_VALUE);
	}
	
	public String getStreamNameFromTableName(String tname ) {
		return kStreamTableMap.get(tname);
	}

	public boolean isKPLAggregationEnabled() {
		return isKPLAggregationEnabled;
	}
	
}
