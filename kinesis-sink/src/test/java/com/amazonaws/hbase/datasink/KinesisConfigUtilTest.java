// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;

public class KinesisConfigUtilTest {
	private static final Logger LOG = LoggerFactory.getLogger(KinesisConfigUtilTest.class);

	private Configuration config;
	private String region="dummy-region-1";
	
	@Before
	public void setUp() throws Exception {
		config = HBaseConfiguration.create();
		try {
			this.region = Regions.getCurrentRegion().getName();
		} catch (Exception e) {
			// not running on AWS
			LOG.info("Test not running on EC2. setting region to: " + this.region);
		}
		config.set("hbase.replication.kinesis.region",this.region);
	}
	 
	@Test
	public void testKinesisEndpoint() throws Exception {
		
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(this.config);
		
		System.out.println(util.getKPLConfiguration().getKinesisEndpoint());
		assertEquals("kinesis."+ this.region +".amazonaws.com",util.getKPLConfiguration().getKinesisEndpoint());
	}
	
	@Test
	public void testRegion() throws Exception {
		config.set("hbase.replication.kinesis.region",this.region);
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(config);
		
		System.out.println("Default Region: " + util.getKPLConfiguration().getRegion() + " Region: " + this.region);
		assertEquals(this.region,util.getKPLConfiguration().getRegion());
	}
	
	@Test
	public void testLogLevel() throws Exception {
		
		config.set("hbase.replication.kinesis.log-level","info");
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(config);
		
		System.out.println("LogLevel: " + util.getKPLConfiguration().getLogLevel());
		assertEquals("info",util.getKPLConfiguration().getLogLevel());
	}
	
	
	@After
    public void stopServer() throws Exception {
 
    } 
	 
}
