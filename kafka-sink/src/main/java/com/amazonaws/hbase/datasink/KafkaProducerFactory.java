// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaProducerFactory {
	
	private static Producer<String, ByteBuffer> producer = null;
	
	public static Producer<String, ByteBuffer> getProducer(Properties config) {
		
		if (producer == null ) {
			producer = new KafkaProducer<>(config);
		} 
		return producer;
    }
}
