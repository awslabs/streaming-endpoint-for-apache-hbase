// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator.model;

import java.util.List;

public class AttributeGroup {
	private byte[] name;
	
	private List<Attribute> attributes;

	public byte[] getName() {
		return name;
	}

	public void setName(byte[] name) {
		this.name = name;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}
	
	public AttributeGroup(String name) {
		this.setName(name.getBytes());
	}
}
