// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class KinesisProducerFactory {
	private static final Logger LOG = LoggerFactory.getLogger(KinesisProducerFactory.class);
	private static KinesisProducer producer = null;
	
	public static KinesisProducer getProducer(KinesisProducerConfiguration config) {
		if ( producer != null ) {
			LOG.debug("Producer is already initialized. returning the old producer.");
			return producer;  
		} else {
			LOG.debug("Initializing a fresh producer.");
			producer = new KinesisProducer(config);
			return producer;
		}
	}
	
	
}


