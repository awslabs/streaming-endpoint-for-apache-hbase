// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsimulator;

import java.util.LinkedList;

import org.apache.hadoop.hbase.util.Bytes;

import com.amazonaws.examples.sensorsimulator.model.Attribute;
import com.amazonaws.examples.sensorsimulator.model.AttributeGroup;
import com.amazonaws.examples.sensorsimulator.model.Row;

public class TrafficRecord extends Row {
	/**
	 * 
	 */
	private static final long serialVersionUID = -402151201783313121L;
	
	public TrafficRecord(String id, long timestamp, float speed, String regNumber) {
		//composite key of id+timestamp
		this.setKey(Bytes.toBytes( id.concat("-"+Long.toString(timestamp))));	
		
		LinkedList<Attribute> atlist = new LinkedList<Attribute>();
		Attribute speedAttr = new Attribute("speed",Float.toString(speed));
		Attribute regoAttr = new Attribute("rego",regNumber);
		atlist.add(speedAttr);
		atlist.add(regoAttr);
		
		AttributeGroup attGroup = new AttributeGroup(Configuration.DEFAULT_HBASE_COLUMNFMILY);
		attGroup.setAttributes(atlist);
		
		LinkedList<AttributeGroup> atGroups = new LinkedList<AttributeGroup>();
		atGroups.add(attGroup);
		
		this.setAttributeGroups(atGroups);
	}
}
