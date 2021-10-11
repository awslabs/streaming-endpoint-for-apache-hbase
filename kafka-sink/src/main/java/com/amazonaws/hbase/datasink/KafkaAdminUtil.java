// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaAdminUtil {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminUtil.class);
	private AdminClient adminClient;
	
	public KafkaAdminUtil(AdminClient adminClient) {
		this.adminClient = adminClient;
	}
	
	public KafkaAdminUtil(Properties config) {
		this.adminClient = AdminClient.create(config);
	}
	
	/**
	 * Creates a topic with 1 patition in Kafka
	 * @param topic
	 * @throws Exception 
	 */
	public void createTopic(final String topic) throws Exception {
		createTopic(topic, 1, (short) 1, Collections.emptyMap());
	}
	
	/**
	 * List topics
	 * @return
	 * @throws Exception
	 */
	public Set<String> listTopics() throws Exception {
		ListTopicsResult topics = adminClient.listTopics();
		Set<String> s = topics.names().get();
		return s;
	}
	
	/**
	 * Creates a topic with partitions and replication.
	 * @param topic
	 * @param partitions
	 * @param replication
	 * @throws Exception 
	 */
	public void createTopic(final String topic, final int partitions, final short replication) throws Exception {
		createTopic(topic, partitions, replication, Collections.emptyMap());
	}

	/**
	 * Create topic with topic options
	 * @param topic
	 * @param partitions
	 * @param replication
	 * @param topicConfig
	 * @throws Exception 
	 */
	public void createTopic(final String topic, final int partitions, final short replication,
			final Map<String, String> topicConfig) throws Exception {
		LOG.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }", topic, partitions,
				replication, topicConfig);

		try {
			final NewTopic newTopic = new NewTopic(topic, partitions, replication);
			newTopic.configs(topicConfig);
			adminClient.createTopics(Collections.singleton(newTopic)).all().get();
		} catch (final InterruptedException | ExecutionException e) {
			throw e;
		}

	}
}
