// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink.model;

import com.fasterxml.jackson.annotation.JsonGetter;

public class Record {
	private String Data;
	private String PartitionKey;
	private String ExplicitHashKey;
	
	@JsonGetter("Data")
	public String getData() {
		return Data;
	}
	
	public void setData(String data) {
		Data = data;
	}
	
	@JsonGetter("PartitionKey")
	public String getPartitionKey() {
		return PartitionKey;
	}
	
	public void setPartitionKey(String partitionKey) {
		PartitionKey = partitionKey;
	}
	
	@JsonGetter("ExplicitHashKey")
	public String getExplicitHashKey() {
		return ExplicitHashKey;
	}
	public void setExplicitHashKey(String explicitHashKey) {
		this.ExplicitHashKey = explicitHashKey;
	}
}
