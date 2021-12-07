// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.serde;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.hbase.model.HBaseCell;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class HBaseCellDeserializer  extends StdDeserializer<HBaseCell>{
	private static final Logger LOG = LoggerFactory.getLogger(HBaseCellDeserializer.class);
	protected HBaseCellDeserializer(Class<?> vc) {
		super(vc);
	}
	
	protected HBaseCellDeserializer() {
		this(null);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 400550195679762658L;

	@Override
	public HBaseCell deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		
		HBaseCell cell = new HBaseCell();
		
		final JsonNode jsonNode = p.getCodec().readTree(p);
		byte[] qualifier = jsonNode.get("qualifier").binaryValue();
		byte[] value = jsonNode.get("value").binaryValue();
		byte[] family = jsonNode.get("family").binaryValue();
		byte[] row = jsonNode.get("row").binaryValue();
		String type = jsonNode.get("type").asText();
		long timestamp = jsonNode.get("timeStamp").asLong();
		
		cell.setFamily(family);
		cell.setQualifier(qualifier);
		cell.setRow(row);
		cell.setTimeStamp(timestamp);
		cell.setType(type);
		cell.setValue(value);
		
		return cell;
	}

}
