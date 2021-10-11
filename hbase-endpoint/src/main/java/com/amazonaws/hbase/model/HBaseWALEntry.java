// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.model;

import java.io.Serializable;

import org.apache.hadoop.hbase.wal.WAL.Entry;

import com.amazonaws.hbase.serde.HBaseWALEntryDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = HBaseWALEntryDeserializer.class)
public class HBaseWALEntry implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3080858007212503033L;

	@JsonProperty("key")
	private HBaseWALKey walKey;
	
	@JsonProperty("edit")
	private HBaseWALEdit walEdit;
	
	
	@JsonCreator
	public HBaseWALEntry(Entry entry) {
		
		assert entry != null;
		this.walKey = new HBaseWALKey(entry.getKey());
		this.walEdit = new HBaseWALEdit(entry.getEdit());
	}
	
	@JsonCreator
	public HBaseWALEntry() { }
	
	@JsonGetter("key")
	public HBaseWALKey getWalKey() {
		return walKey;
	}

	@JsonSetter("key")
	public void setWalKey(HBaseWALKey walKey) {
		this.walKey = walKey;
	}
	
	@JsonGetter("edit")
	public HBaseWALEdit getWalEdit() {
		return walEdit;
	}
	
	@JsonSetter("edit")
	public void setWalEdit(HBaseWALEdit walEdit) {
		this.walEdit = walEdit;
	}
}
