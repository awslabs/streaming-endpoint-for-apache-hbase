// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.hbase.model.HBaseWALEntry;

public class StreamingReplicationEndpoint extends BaseReplicationEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingReplicationEndpoint.class);
	 
	protected DataSink dataSink = null; 

	protected ConfigurationUtil configUtil = null;
	protected WALEntryFilter filters;

	protected ObjectMapper objectMapper = new ObjectMapper();
	Connection localConn;
	Admin localAdmin;

	protected MetricsSource metrics;

	public StreamingReplicationEndpoint() {
	}

	@Override
	public void init(Context context) throws IOException {
		this.ctx = context;

		this.configUtil = new ConfigurationUtil(this.ctx.getConfiguration());
		try {
			this.dataSink = new DataSinkFactory.Builder().withConfiguration(this.ctx.getConfiguration()).build();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Unable to initialize dataSink. Make sure the Data sink implementaion is in classpath." + e.getMessage(),e);
		}

		this.metrics = context.getMetrics();

		this.filters = this.getWALEntryfilter();

		objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);


		localConn = ConnectionFactory.createConnection(ctx.getLocalConfiguration());
		localAdmin = localConn.getAdmin();
	}

	@Override
	protected void doStart() {

		// Required to ensure that HBase knows the endpoint has started
		notifyStarted();
	}

	@Override
	protected void doStop() {
		// Required to ensure that HBase knows the endpoint has stopped
		dataSink.flush();
		notifyStopped();
	}

	// This is a streaming replications and there is no adjacent or remote clusters. 
	@Override
	public boolean canReplicateToSameCluster() {
		return true;
	}

	@Override
	public boolean replicate(ReplicateContext replicateContext) {
		LOG.info("replication length: " + replicateContext.getSize());
		LOG.info("replication entries: " + replicateContext.getEntries().size());
		String WALId = replicateContext.getWalGroupId();

		List<Entry> entries = new LinkedList<Entry>();
		//List<Entry> oldEntries = this.filterNotMappedToStream(replicateContext.getEntries());
		List<Entry> oldEntries=this.filterNotExistColumnFamilyEdits(this.filterNotExistTableEdits(replicateContext.getEntries()));
			
		for (Entry e: this.dataSink.filter(oldEntries)) {
			Entry tmp = filters.filter(e);
			if ( tmp != null ) { 
				entries.add(tmp); 
				LOG.debug("Replication entry added:" + e.getKey().toString());
			} else {
				LOG.debug("Replication entry Filtered:" + e.getKey().toString());
				metrics.incrLogEditsFiltered();
				
			}
		}

		LOG.debug("entry size Before filter " + oldEntries.size() + " after filters:" + entries.size());

		try {
			if ( dataSink.supportsTransaction()) {
				dataSink.beginTransaction();
			}

			for (Entry entry : entries) {
				String tname = entry.getKey().getTableName().getNameAsString();
				HBaseWALEntry hbaseWALEntry = new HBaseWALEntry(entry);
				
				ByteBuffer data;
				try {
					byte[] outputData;
					if (this.configUtil.isCompressionEnabled()) {
						outputData = gzipCompress(objectMapper.writeValueAsString(hbaseWALEntry).getBytes("UTF-8"));
					} else {
						outputData = objectMapper.writeValueAsString(hbaseWALEntry).getBytes("UTF-8");
					}
					
					data = ByteBuffer.wrap(outputData);
					dataSink.putRecord(data,tname);
					metrics.incrCompletedWAL();
					metrics.setAgeOfLastShippedOp(hbaseWALEntry.getWalKey().getWriteTime(), WALId);
					metrics.setAgeOfLastShippedOpByTable(hbaseWALEntry.getWalKey().getWriteTime(),hbaseWALEntry.getWalKey().getTableName());
				} catch (UnsupportedEncodingException e1) {
					LOG.error("Encoding is set to UTF-8 but it is not supported. " + " " + formatStackTrace(e1));
					dataSink.abortTransaction();
					throw new RuntimeException(e1.getStackTrace().toString());
				} catch (JsonProcessingException e1) {
					LOG.error("Object could not be converted to json" + " " + formatStackTrace(e1));
					dataSink.abortTransaction();
					throw new RuntimeException(e1.getStackTrace().toString());
				} catch (IOException e1) {
					e1.printStackTrace();
					dataSink.abortTransaction();
					return false;
				}
			}
			dataSink.commitTransaction();
		} catch ( Exception e ) {
			e.printStackTrace();
			LOG.error("Unhandled Exception: " + e.getMessage() + " " + formatStackTrace(e));
			return false;
		}

		return true;
	}

	@Override
	public void start() {
		startAsync();
	}

	@Override
	public void stop() {
		dataSink.flush();

		stopAsync();
	}
	
	/**
	 * Returns GZip compressed of a byte[]
	 * 
	 * @param bytes bytes to compress
	 * @return 		gzip compressed results of input
	 * @throws IOException
	 */
	private byte[] gzipCompress(byte[] bytes) throws IOException {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bytes.length);
		GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
		zipStream.write(bytes);
		zipStream.close();
		byteStream.close();
		return byteStream.toByteArray();
		
	}

	protected List<Entry> filterNotExistColumnFamilyEdits(final List<Entry> oldEntries) {
		if (configUtil.isDropOnDeletedColumnFamilies() == false) {
			return oldEntries;
		}
		List<Entry> entries = new ArrayList<Entry>();
		Map<TableName, Set<byte[]>> existColumnFamilyMap = new HashMap<>();


		for (Entry e : oldEntries ) {
			TableName tableName = e.getKey().getTableName();
			if (!existColumnFamilyMap.containsKey(tableName)) {
				try {
					Set<byte[]> cfs = localAdmin.getDescriptor(tableName).getColumnFamilyNames();
					existColumnFamilyMap.put(tableName, cfs);
				} catch (Exception ex) {
					LOG.warn("Exception getting cf names for local table {}", tableName, ex);
					// if catch any exception, we are not sure about table's description,
					// so replicate raw entry
					entries.add(e);
					continue;
				}
			}

			Set<byte[]> existColumnFamilies = existColumnFamilyMap.get(tableName);
			Set<byte[]> missingCFs = new HashSet<>();
			WALEdit walEdit = new WALEdit();

			for (Cell cell: e.getEdit().getCells() ) {
				if (configUtil.isReplicationBulkLoadDataEnabled() && Bytes.equals(cell.getQualifierArray(),"HBASE::BULK_LOAD".getBytes())) {
					walEdit.add(cell);
					continue;
				}
				if (existColumnFamilies.contains(CellUtil.cloneFamily(cell))) {
					walEdit.add(cell);
				} else {
					missingCFs.add(CellUtil.cloneFamily(cell));
				}
			}

			if (!walEdit.isEmpty()) {
				Entry newEntry = new Entry(e.getKey(), walEdit);
				entries.add(newEntry);
			}

			if (!missingCFs.isEmpty()) {
				// Would potentially be better to retry in one of the outer loops
				// and add a table filter there; but that would break the encapsulation,
				// so we're doing the filtering here.
				LOG.warn(
						"Missing column family detected at replicate, local column family also does not exist,"
								+ " filtering edits for table '{}',column family '{}'", tableName, missingCFs);
			}
		}
		long count = oldEntries.size() - entries.size();
		if (count > 0) {
			LOG.warn("ColumnFamilyFitred records: {}",Long.toString(count));
		}
		return entries;
	}

	@Override
	public UUID getPeerUUID() {
		return this.ctx.getClusterId();
	}

	protected List<Entry> filterNotExistTableEdits(final List<Entry> oldEntries) {
		if (configUtil.isDropOnDeletedTables() == false) {
			return oldEntries;
		}
		List<Entry> entries = new ArrayList<>();
		Map<TableName, Boolean> existMap = new HashMap<>();

		for (Entry e : oldEntries) {
			TableName tableName = e.getKey().getTableName();
			boolean exist = true;
			if (existMap.containsKey(tableName)) {
				exist = existMap.get(tableName);
			} else {
				try {
					exist = localAdmin.tableExists(tableName);
					existMap.put(tableName, exist);
				} catch (IOException iox) {
					LOG.warn("Exception checking for local table " + tableName + " " + formatStackTrace(iox));
					// we can't drop edits without full assurance, so we assume table exists.
					exist = true;
				}
			}
			if (exist) {
				entries.add(e);
			} else {
				// Would potentially be better to retry in one of the outer loops
				// and add a table filter there; but that would break the encapsulation,
				// so we're doing the filtering here.
				LOG.warn("Missing table detected at replication, local table does not exist, "
						+ "filtering edits for table '{}'", tableName);
			}
		}
		long count = oldEntries.size() - entries.size();
		if (count > 0) {
			LOG.warn("DropppedTableFitred records: {}",Long.toString(count));
		}
		return entries;
	}

	private String formatStackTrace(Exception ex) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		ex.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}
}
