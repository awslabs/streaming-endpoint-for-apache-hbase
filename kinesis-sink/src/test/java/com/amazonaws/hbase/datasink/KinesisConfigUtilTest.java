// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.amazonaws.regions.Regions;

public class KinesisConfigUtilTest {

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
		String region = Regions.getCurrentRegion().getName();

		config.set("hbase.replication.kinesis.region","ap-southeast-2");
		KinesisConfigurationUtil util = new KinesisConfigurationUtil(config);
		
		System.out.println("Default Region: " + util.getKPLConfiguration().getRegion() + " Region: " + region);
		assertEquals("ap-southeast-2",util.getKPLConfiguration().getRegion());
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
