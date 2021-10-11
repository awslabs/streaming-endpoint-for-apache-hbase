// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator.model;

import java.io.Serializable;
import java.util.List;

public abstract class Row implements Serializable {

	private static final long serialVersionUID = -8836568249633341128L;
	
	private byte[] key;
	private List<AttributeGroup> attributeGroups;
	
	public List<AttributeGroup> getAttributeGroups() {
		return attributeGroups;
	}
	public void setAttributeGroups(List<AttributeGroup> attributeGroups) {
		this.attributeGroups = attributeGroups;
	}
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] bs) {
		this.key = bs;
	}
}

