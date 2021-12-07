// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.serde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.hbase.model.HBaseWALEdit;
import com.amazonaws.hbase.model.HBaseCell;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;


public class HBaseWALEditDeserializer extends StdDeserializer<HBaseWALEdit> {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseWALEditDeserializer.class);
	protected HBaseWALEditDeserializer(Class<?> vc) {
		super(vc);
	}

	protected HBaseWALEditDeserializer() {
		this(null);
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6140869646768938288L;

	@Override
	public HBaseWALEdit<HBaseWALEdit> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		ArrayList<HBaseCell> cells = new ArrayList<HBaseCell>();
		Set<byte[]> families = new HashSet<byte[]>();
		byte[] METAFAMILY;
		boolean replay;
		
		JsonNode jsonNode = p.getCodec().readTree(p);
		
		final JsonNode cellsNode = jsonNode.get("cells");
		if (cellsNode.isArray()) {
		    for (final JsonNode objNode : cellsNode) {
		    	        cells.add(p.getCodec().treeToValue(objNode, HBaseCell.class));
		    }
		}
		
		
		final JsonNode familiesNode = jsonNode.get("families");
		if (familiesNode.isArray()) {
			familiesNode.elements().forEachRemaining((JsonNode node) -> {
				try {
					families.add(node.binaryValue());
				} catch (IOException e) {
					LOG.warn("Unable to deserialize all the family names from array.",e);
					e.printStackTrace();
				}
			});					

		}
		
		METAFAMILY=jsonNode.get("metafamily").binaryValue();
		
		replay = jsonNode.get("replay").asBoolean();
		
		HBaseWALEdit<HBaseWALEdit> hBaseEdit = new HBaseWALEdit<HBaseWALEdit>();
		hBaseEdit.setCells(cells);
		hBaseEdit.setFamilies(families);
		hBaseEdit.setMETAFAMILY(METAFAMILY);
		hBaseEdit.setReplay(replay);
		
		return hBaseEdit;
	}
}
