// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink.model;

import java.util.ArrayList;

import com.amazonaws.hbase.datasink.model.RecordResult;
import com.fasterxml.jackson.annotation.JsonSetter;

public class KinesisPutRecordsResponse {
	private int FailedRecordCount;
	private ArrayList<RecordResult> Records;
	
	
	public ArrayList<RecordResult> getRecords() {
		return Records;
	}
	
	@JsonSetter("Records")
	public void setRecords(ArrayList<RecordResult> Records) {
		this.Records = Records;
	}
	
	
	public int getFailedRecordCount() {
		return FailedRecordCount;
	}
	
	@JsonSetter("FailedRecordCount")
	public void setFailedRecordCount(int FailedRecordCount) {
		this.FailedRecordCount = FailedRecordCount;
	}
}
