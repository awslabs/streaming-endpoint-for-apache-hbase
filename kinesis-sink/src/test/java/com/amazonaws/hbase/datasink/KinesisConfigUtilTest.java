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
	@Before
	public void setUp() throws Exception {
		config = HBaseConfiguration.create();
	}
	 
	@Test
	public void testKinesisEndpoint() throws Exception {
		
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(config);
		System.out.println(util.getKPLConfiguration().getKinesisEndpoint());
		
		
		config.set("hbase.replication.kinesis.region","us-east-2");
		util = new KinesisConfigurationUtil(this.config);
		
		System.out.println(util.getKPLConfiguration().getKinesisEndpoint());
		assertEquals("kinesis.us-east-2.amazonaws.com",util.getKPLConfiguration().getKinesisEndpoint());
	}
	
	@Test
	public void testRegion() throws Exception {
		String region="dummy-region";
		try {
			region = Regions.getCurrentRegion().getName();
		} catch (SdkClientException e) {
			// not running on AWS
			LOG.info("Test not running on EC2. setting region to: " + region);
			
		}
		config.set("hbase.replication.kinesis.region",region);
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(config);
		
		System.out.println("Default Region: " + util.getKPLConfiguration().getRegion() + " Region: " + region);
		assertEquals(region,util.getKPLConfiguration().getRegion());
	}
	
	@Test
	public void testLogLevel() throws Exception {
		
		config.set("hbase.replication.kinesis.log-level","debug");
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(config);
		
		System.out.println("LogLEvel: " + util.getKPLConfiguration().getLogLevel());
		assertEquals("debug",util.getKPLConfiguration().getLogLevel());
	}
	
	
	@After
    public void stopServer() throws Exception {
 
    } 
	 
}
