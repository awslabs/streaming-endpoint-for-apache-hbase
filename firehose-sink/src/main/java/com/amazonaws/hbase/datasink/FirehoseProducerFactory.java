// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;

public class FirehoseProducerFactory {
	private static final Logger LOG = LoggerFactory.getLogger(FirehoseProducerFactory.class);
	private static AmazonKinesisFirehoseClientBuilder client = null;
		
	public static AmazonKinesisFirehose getProducer(Configuration config) {
		FirehoseConfigurationUtil confUtil = new FirehoseConfigurationUtil(config);
		if ( client != null ) {
			LOG.debug("Firehose Client is already initialized. returning the old producer.");
			return client.build();  
		} else {
			LOG.debug("Initializing a fresh producer.");
			client = AmazonKinesisFirehoseClientBuilder.standard();
			client.setRegion(confUtil.getFirehoseRegion());
			return client.build();
		}
	}
}