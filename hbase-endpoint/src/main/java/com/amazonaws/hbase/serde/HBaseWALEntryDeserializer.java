// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.serde;

import java.io.IOException;

import com.amazonaws.hbase.model.HBaseCell;
import com.amazonaws.hbase.model.HBaseWALEdit;
import com.amazonaws.hbase.model.HBaseWALEntry;
import com.amazonaws.hbase.model.HBaseWALKey;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class HBaseWALEntryDeserializer extends StdDeserializer<HBaseWALEntry> {

	protected HBaseWALEntryDeserializer(Class<?> vc) {
		super(vc);
	}
	
	protected HBaseWALEntryDeserializer() {
		this(null);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -299530848693054619L;

	@Override
	public HBaseWALEntry deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		
		HBaseWALEntry entry = new HBaseWALEntry();
		
		HBaseWALKey key = new HBaseWALKey();
		HBaseWALEdit<HBaseCell> edit = new HBaseWALEdit<HBaseCell>(); 
		
		JsonNode jsonNode = p.getCodec().readTree(p);
		
		final JsonNode jkey = jsonNode.get("key");
		key = p.getCodec().treeToValue(jkey, HBaseWALKey.class);
        
        final JsonNode jedit = jsonNode.get("edit");
		edit = p.getCodec().treeToValue(jedit, HBaseWALEdit.class);
		
		entry.setWalEdit(edit);
		entry.setWalKey(key);
		
		return entry;
	}

}
