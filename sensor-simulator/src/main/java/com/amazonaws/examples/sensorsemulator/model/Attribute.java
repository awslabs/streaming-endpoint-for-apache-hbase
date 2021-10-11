// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator.model;

public class Attribute {
	private byte[] key;
	private byte[] value;
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	
	public Attribute(String key, String value) {
		this.setKey(key.getBytes());
		this.setValue(value.getBytes());
	}
}
