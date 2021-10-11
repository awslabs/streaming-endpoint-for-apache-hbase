// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
				
import org.apache.hadoop.hbase.client.Put;							
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.examples.sensorsemulator.model.Attribute;
import com.amazonaws.examples.sensorsemulator.model.AttributeGroup;
import com.amazonaws.examples.sensorsemulator.model.Row;
import com.fasterxml.jackson.core.util.TextBuffer;


public class HbaseSensorSink implements DestinationSink {
	
	private static final Logger LOG = LoggerFactory.getLogger(HbaseSensorSink.class);
	
	

	private org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
	private TableName tableName = TableName.valueOf(Configuration.DEFAULT_HBASE_TABLENAME);	

	DrainThread drainThread = new DrainThread();
	
	private TableDescriptor tDesc = null;
	private Connection connection = null; 
	private Admin admin;
	
	
   
	private LinkedBlockingDeque<Put> putBuffer = new LinkedBlockingDeque<Put>(100);
	
	public boolean createReplictionPeer( ) throws IOException {
		admin.addReplicationPeer("StreamingReplication", new ReplicationPeerConfig().setReplicationEndpointImpl("com.amazonaws.hbase.StreamingReplicationEndpoint"), true);
		return true;
	}
	
	public boolean createTableWithReplication(TableName tablename) {
		try {
			
			// Create table if doesn't exist and add the column family if found table
			// doesn't have the ColumnFamily.
			if (admin.tableExists(tableName)) {
				tDesc = admin.getDescriptor(tableName);
			}
			
			//Create ColumnFamily with replication Enabled. 
			ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
					.newBuilder(Configuration.DEFAULT_HBASE_COLUMNFMILY.getBytes()).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build();
			
			//Create SeconfColumnFamily without replication Enabled. 
			ColumnFamilyDescriptor secondcolumnFamilyDescriptor = ColumnFamilyDescriptorBuilder
					.newBuilder(Configuration.DEFAULT_HBASE_ENRICH_CULUMNFAMILY.getBytes()).build();
			
			//If table doesn't exist, create the table. Otherwise check if the table has the column family and add the ColumnFamily. 
			if (tDesc != null) {
				if (tDesc.hasColumnFamily(Configuration.DEFAULT_HBASE_COLUMNFMILY.getBytes()) == false) {
					admin.disableTable(tDesc.getTableName());
					admin.addColumnFamily(tDesc.getTableName(), columnFamilyDescriptor);
					admin.enableTable(tDesc.getTableName());
				}
			} else {
				TableDescriptorBuilder tb = TableDescriptorBuilder.newBuilder(tableName);
				List<ColumnFamilyDescriptor> cfList = new ArrayList<ColumnFamilyDescriptor>();
				cfList.add(columnFamilyDescriptor);
				cfList.add(secondcolumnFamilyDescriptor);
				tb.setColumnFamilies(cfList);
				
				tDesc = tb.build();
				admin.createTable(tDesc);

			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public HbaseSensorSink(String tname) throws IOException {
		
		this.tableName = TableName.valueOf(tname);
		
		String path = this.getClass()
				  .getClassLoader()
				  .getResource("hbase-site.xml")
				  .getPath();
		configuration.addResource(new Path(path));
		
		// throw and exception if HBase is not available.
		HBaseAdmin.available(configuration);
		this.connection = ConnectionFactory.createConnection(configuration);

		admin = this.connection.getAdmin();

		this.createTableWithReplication(tableName);
		this.createReplictionPeer();
		
		//Start DrainThread
		Thread thread = new Thread(drainThread);
		thread.start();
	}
	
	public HbaseSensorSink() throws IOException {
		this(Configuration.DEFAULT_HBASE_TABLENAME);
	}
	
	public boolean write(Row record) {
		Put p = new Put(record.getKey());
		for (AttributeGroup cfamily: record.getAttributeGroups()) {
			for (Attribute attr: cfamily.getAttributes()) {
				p.addColumn(cfamily.getName(), attr.getKey(), attr.getValue());
			}
		}
		try {
			putBuffer.add(p);
		} catch ( IllegalStateException e) {
				LOG.warn("put buffer full!");
				return false;
		}
		return true;
	}
	
	public class DrainThread implements Runnable {
		public Thread thread = null;
		long counter=0L;
		public void run() {
			try {
				Table table = connection.getTable(tableName);
				while ( true ) {
					table.put(putBuffer.poll(1,TimeUnit.MINUTES));
					counter++;
					
					if (counter%10000 == 0) {
						LOG.info("Complete puts: " + Long.toString(counter));
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.exit(1);
		}
	}
}

