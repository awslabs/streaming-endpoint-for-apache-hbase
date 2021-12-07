// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.model;

import java.io.Serializable;
import java.util.Optional;

import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

public class MVCCWALEntry implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7917422354653462119L;

	@JsonProperty("writeNumber")
	private Long writeNumber=null;
	
	@JsonCreator
	public MVCCWALEntry(MultiVersionConcurrencyControl.WriteEntry entry) {
		if (entry!=null) {
			this.writeNumber = entry.getWriteNumber();
		}
	}
	@JsonCreator
	public MVCCWALEntry() {	}
	@JsonGetter("writeNumber")
	public long getWriteNumber() {
		return writeNumber;
	}
	
	@JsonSetter("writeNumber")
	public void setWriteNumber(long writeNumber) {
		this.writeNumber = writeNumber;
	}

}
