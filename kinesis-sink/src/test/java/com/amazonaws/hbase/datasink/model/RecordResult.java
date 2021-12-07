// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink.model;

import com.fasterxml.jackson.annotation.JsonSetter;

public class RecordResult {
	private String SequenceNumber;
	private String ShardId;
	
	public String getShardId() {
		return ShardId;
	}
	
	@JsonSetter("ShardId")
	public void setShardId(String ShardId) {
		this.ShardId = ShardId;
	}
	
	
	public String getSequenceNumber() {
		return SequenceNumber;
	}
	
	@JsonSetter("SequenceNumber")
	public void setSequenceNumber(String SequenceNumber) {
		this.SequenceNumber = SequenceNumber;
	}

}
