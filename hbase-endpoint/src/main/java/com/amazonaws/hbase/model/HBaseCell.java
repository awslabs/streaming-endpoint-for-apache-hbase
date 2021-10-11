// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;


import com.amazonaws.hbase.serde.HBaseCellDeserializer;


@JsonDeserialize(using = HBaseCellDeserializer.class)
public class HBaseCell implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3069310732828382967L;


	private byte[] qualifier;
	private byte[] value;
	private String type;
	private byte[] family;
	private long timeStamp;
	private byte[] row;
	
	@JsonCreator
	public HBaseCell(Cell cell) {
		
		assert cell !=null;
		
		setQualifier(extractBytes(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
		setValue(extractBytes(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
		setFamily(extractBytes(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()));
		setRow(extractBytes(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()));
	
		setTimeStamp(cell.getTimestamp());
		setType(cell.getType().toString());
	}
	
	@JsonCreator
	public HBaseCell() { }
	
	@JsonGetter("timeStamp")
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	@JsonGetter("type")
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	@JsonGetter("value")
	public byte[] getValue() {
		return value;
	}
	
	public void setValue(byte[] value) {
		this.value = value;
	}
	
	@JsonGetter("qualifier")
	public byte[] getQualifier() {
		return qualifier;
	}
	
	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}
	
	@JsonGetter("family")
	public byte[] getFamily() {
		return family;
	}
	
	public void setFamily(byte[] family) {
		this.family = family;
	}
	
	@JsonGetter("row")
	public byte[] getRow() {
		return row;
	}
	
	public void setRow(byte[] row) {
		this.row = row;
	}
	
	private byte[] extractBytes(byte[] arr, int offset, int length) {
		byte[] res = Arrays.copyOfRange(arr,offset,offset+length);
		return res;
	}
}