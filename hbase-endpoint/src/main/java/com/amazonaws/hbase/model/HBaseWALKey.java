// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.model;

import java.io.Serializable;


import org.apache.hadoop.hbase.wal.WALKeyImpl;

import com.amazonaws.hbase.serde.HBaseWALKeyDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;


@JsonDeserialize(using = HBaseWALKeyDeserializer.class)
@JsonIgnoreProperties(value= {"replicationScope","extendedAttributes" })
public class HBaseWALKey implements Serializable{
	
	@JsonProperty("writeTime")
	private Long writeTime;
	
	@JsonProperty("sequenceId")
	private Long sequenceId;
	
	@JsonProperty("tablename")
	private String tablename;
	
	@JsonProperty("nonce")
	private Long nonce;
	
	@JsonProperty("nonceGroup")
	private Long nonceGroup;
	
	@JsonProperty("origLogSeqNum")
	private Long origLogSeqNum;
	
	@JsonProperty("encodedRegionName")
	private byte[] encodedRegionName;
	
	@JsonProperty("writeEntry")

	private MVCCWALEntry writeEntry=null;
	
	
	@JsonCreator
	public HBaseWALKey(WALKeyImpl walKey) {
		
		assert walKey != null;
		
		this.writeTime = walKey.getWriteTime();
		this.sequenceId = walKey.getSequenceId();
		this.tablename = walKey.getTableName().getNameAsString();
		this.encodedRegionName = walKey.getEncodedRegionName();
		this.nonce = walKey.getNonce();
		this.nonceGroup = walKey.getNonceGroup();
		this.origLogSeqNum = walKey.getOrigLogSeqNum();
		if (walKey.getWriteEntry() != null) {
			this.writeEntry = new MVCCWALEntry(walKey.getWriteEntry());
		}
		
	}
	
	@JsonCreator
	public HBaseWALKey() { }
	
	@JsonGetter("writeTime")
	public Long getWriteTime() {
		return writeTime;
	}
	
	@JsonSetter("writeTime")
	public void setWriteTime(Long writeTime) {
		this.writeTime = writeTime;
	}
	
	@JsonGetter("sequenceId")
	public Long getSequenceId() {
		return sequenceId;
	}
	@JsonSetter("sequenceId")
	public void setSequenceId(Long sequenceId) {
		this.sequenceId = sequenceId;
	}
	
	@JsonGetter("tablename")
	public String getTableName() {
		return tablename;
	}
	
	@JsonSetter("tablename")
	public void setTableName(String tableName) {
		this.tablename = tableName;
	}

	@JsonGetter("nonce")
	public Long getNonce() {
		return nonce;
	}

	@JsonSetter("nonce")
	public void setNonce(Long nonce) {
		this.nonce = nonce;
	}

	@JsonGetter("nonceGroup")
	public Long getNonceGroup() {
		return nonceGroup;
	}

	@JsonSetter("nonceGroup")
	public void setNonceGroup(Long nonceGroup) {
		this.nonceGroup = nonceGroup;
	}

	@JsonGetter("origLogSeqNum")
	public Long getOrigLogSeqNum() {
		return origLogSeqNum;
	}

	@JsonSetter("origLogSeqNum")
	public void setOrigLogSeqNum(Long origLogSeqNum) {
		this.origLogSeqNum = origLogSeqNum;
	}

	@JsonGetter("encodedRegionName")
	public byte[] getEncodedRegionName() {
		return encodedRegionName;
	}

	@JsonSetter("encodedRegionName")
	public void setEncodedRegionName(byte[] encodedRegionName) {
		this.encodedRegionName = encodedRegionName;
	}

	@JsonGetter("writeEntry")
	public MVCCWALEntry getWriteEntry() {
		return writeEntry;
	}

	@JsonSetter("writeEntry")
	public void setWriteEntry(MVCCWALEntry writeEntry) {
		this.writeEntry = writeEntry;
	}
}
