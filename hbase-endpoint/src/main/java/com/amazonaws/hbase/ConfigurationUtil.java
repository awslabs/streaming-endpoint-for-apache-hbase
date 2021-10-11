// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

//import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;



public class ConfigurationUtil {

	public static final String BASE_HBASE = "hbase.replication";
	/** Drop edits for tables that been deleted from the replication source and target */
	public static final String REPLICATION_DROP_ON_DELETED_TABLE_KEY =
			"hbase.replication.drop.on.deleted.table";
	/** Drop edits for CFs that been deleted from the replication source and target */
	public static final String REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY =
			"hbase.replication.drop.on.deleted.columnfamily";

	public static final String SINK_IMPLENEMTATION_FACTORY =
			BASE_HBASE+".sink-factory-class";
	
	public static final String COMPRESSION_ENABLED = 
			BASE_HBASE+".compression-enabled";
	
	protected Configuration conf;
	
	private boolean dropOnDeletedTables;
	private boolean dropOnDeletedColumnFamilies;
	private boolean replicationBulkLoadDataEnabled;

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtil.class);
	
	public ConfigurationUtil (Configuration conf) {
		this.conf = conf;
		
		this.dropOnDeletedTables =
				this.conf.getBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
		
		this.dropOnDeletedColumnFamilies = this.conf
				.getBoolean(REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY, false);

		this.replicationBulkLoadDataEnabled =
				conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
						HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
		
		
		//Path rootDir = CommonFSUtils.getRootDir(conf);
		//Path baseNSDir = new Path(HConstants.BASE_NAMESPACE_DIR);
		//Path baseNamespaceDir = new Path(rootDir, baseNSDir);
		//Path hfileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNSDir));
		
	}

	public boolean isDropOnDeletedTables() {
		return dropOnDeletedTables;
	}

	public boolean isReplicationBulkLoadDataEnabled() {
		return replicationBulkLoadDataEnabled;
	}

	public boolean isDropOnDeletedColumnFamilies() {
		return dropOnDeletedColumnFamilies;
	}
	
	public String getSinkImplementationFacotry() {
		String fName = this.conf.get(SINK_IMPLENEMTATION_FACTORY);
		if (fName == null)  {
			LOG.error(SINK_IMPLENEMTATION_FACTORY + " for Streaming replication can't be null!");
			throw new RuntimeException(SINK_IMPLENEMTATION_FACTORY + " for Streaming replication can't be null!");
		}
		return this.conf.get(SINK_IMPLENEMTATION_FACTORY);
	}
	
	public boolean isCompressionEnabled() {
		return this.conf.getBoolean(COMPRESSION_ENABLED,false);
	}
	
}
