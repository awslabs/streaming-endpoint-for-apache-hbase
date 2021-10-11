// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator;

public  class Configuration {
	public final static int DEFAULT_SENSOR_COUNT=20;
	public final static int DEFAULT_SAMPLES_PERSECOND=30;
	public final static int DEFAULT_THREAD_COUNT=1;
	public final static String DEFAULT_HBASE_TABLENAME = "demosensors";
	public final static String DEFAULT_HBASE_COLUMNFMILY = "democf";
	public final static String DEFAULT_HBASE_ENRICH_CULUMNFAMILY = "enrich";
	public final static int RETRY_THRESHOLD = 3;
}
