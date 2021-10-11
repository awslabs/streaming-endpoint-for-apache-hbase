// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.model;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.amazonaws.hbase.serde.HBaseCellDeserializer;
import com.amazonaws.hbase.serde.HBaseWALEditDeserializer;


import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@JsonDeserialize(using = HBaseWALEditDeserializer.class)
public class HBaseWALEdit<T> implements Serializable {
	private static final long serialVersionUID = -3695971167206715504L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseWALEdit.class);
	
	private List<HBaseCell> cells;

	
	private Set<byte[]> families;
	
	private byte[] METAFAMILY;
	
	private boolean replay;
	
	
	@JsonGetter("cells")
	public List<HBaseCell> getCells() {
		return cells;
	}

	public void setCells(List<HBaseCell> cells) {
		this.cells = cells;
	}

	@JsonCreator
	public HBaseWALEdit(@JsonProperty("WALEdit") WALEdit edit) {
		
		assert edit!=null;
		
		this.cells = new LinkedList<HBaseCell>();
		for (Cell index: edit.getCells() ) {
				HBaseCell c = new HBaseCell((Cell)index);
				this.cells.add(c);
		}
		
		this.families = edit.getFamilies();
		this.replay = edit.isReplay();
		this.METAFAMILY = edit.METAFAMILY;
	}
	
	@JsonCreator
	public HBaseWALEdit() {	}

	@JsonGetter("families")
	public Set<byte[]> getFamilies() {
		return families;
	}

	public void setFamilies(Set<byte[]> families) {
		this.families = families;
	}

	@JsonGetter("metafamily")
	public byte[] getMETAFAMILY() {
		return METAFAMILY;
	}

	public void setMETAFAMILY(byte[] mETAFAMILY) {
		METAFAMILY = mETAFAMILY;
	}

	@JsonGetter("replay")
	public boolean getReplay() {
		return replay;
	}

	public void setReplay(boolean replay) {
		this.replay = replay;
	}
}
