// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink.model;

import java.util.ArrayList;

import com.amazonaws.hbase.datasink.model.Record;
import com.fasterxml.jackson.annotation.JsonGetter;

public class KinesisPutRecordsRequest {
	private ArrayList<Record> records;
	private String streamName;
	
	@JsonGetter("Records")
	public ArrayList<Record> getRecords() {
		return records;
	}
	
	public void setRecords(ArrayList<Record> records) {
		this.records = records;
	}

	@JsonGetter("StreamName")
	public String getStreamName() {
		return streamName;
	}
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}
}
