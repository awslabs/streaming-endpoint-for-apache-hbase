// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.serde;

import java.io.IOException;

import org.apache.hadoop.hbase.wal.WALKeyImpl;

import com.amazonaws.hbase.model.HBaseWALEdit;
import com.amazonaws.hbase.model.HBaseWALKey;
import com.amazonaws.hbase.model.MVCCWALEntry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class HBaseWALKeyDeserializer extends StdDeserializer<HBaseWALKey>{

	protected HBaseWALKeyDeserializer() {
		this(null);
	}

	public HBaseWALKeyDeserializer(Class<?> object) {
		super(object);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7035821190664592174L;

	@Override
	public HBaseWALKey deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		HBaseWALKey res = new HBaseWALKey();
		
		Long writeTime;
		Long sequenceId;
		String tablename;
		Long nonce;
		Long nonceGroup;
		Long origLogSeqNum;
		byte[] encodedRegionName;
		MVCCWALEntry writeEntry;
		
		JsonNode jsonNode = p.getCodec().readTree(p);
		
		encodedRegionName = jsonNode.get("encodedRegionName").binaryValue();
		nonce = jsonNode.get("nonce").asLong();
		nonceGroup =jsonNode.get("nonceGroup").asLong();
		writeTime = jsonNode.get("writeTime").asLong();
		sequenceId = jsonNode.get("sequenceId").asLong();
		tablename = jsonNode.get("tablename").asText();
		writeEntry = objectMapper.readValue(jsonNode.get("writeEntry").asText(), MVCCWALEntry.class);
		origLogSeqNum = jsonNode.get("origLogSeqNum").asLong();
		
		res.setEncodedRegionName(encodedRegionName);
		res.setNonce(nonce);
		res.setNonceGroup(nonceGroup);
		res.setOrigLogSeqNum(origLogSeqNum);
		res.setSequenceId(sequenceId);
		res.setTableName(tablename);
		res.setWriteEntry(writeEntry);
		res.setWriteTime(writeTime);
		
		return res;
	}
	

}
