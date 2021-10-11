// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.lambda;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupType;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.hbase.model.HBaseCell;
import com.amazonaws.hbase.model.HBaseWALEntry;
import com.amazonaws.regions.Regions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

// Handler value: example.HandleKinesis
public class KinesisHandler implements RequestHandler<KinesisEvent, String> {
	private LambdaLogger logger;
	private ObjectMapper mapper = new ObjectMapper();
	private RecordValidator validator = new RecordValidator();
	private String clusterId=System.getenv("CLUSTERID");
	private String tName=System.getenv("TABLENAME");
	private boolean compressionEnabled = Boolean.getBoolean(System.getenv("COMPRESSIONENABLED"));
	
	private final static String DEFAULT_HBASE_ENRICH_CULUMNFAMILY = "enrich";
	
	private Configuration configuration = HBaseConfiguration.create();
	private TableName tableName = TableName.valueOf(tName);	

	private Connection connection = null; 

	private Table table = null;
	
	private String masterPrivateIpAddr = getMasterInstanceIpAddress(clusterId,System.getenv("AWS_REGION"));
	
	public KinesisHandler() throws Exception {
		String path = this.getClass()
				  .getClassLoader()
				  .getResource("hbase-site.xml")
				  .getPath();
		configuration.addResource(new Path(path));
		configuration.set("hbase.zookeeper.quorum",masterPrivateIpAddr );
		
		// throw and exception if Hbase is not available.
		HBaseAdmin.available(configuration);
		this.connection = ConnectionFactory.createConnection(configuration);

		table = connection.getTable(tableName);
		
	}
	
	@Override
	public String handleRequest(KinesisEvent event, Context context) {
		logger =  context.getLogger();
		String response = new String("200 OK");
		List<KinesisEventRecord>  records = event.getRecords();
		//logger.log("Record count: " + records.size());
		for (KinesisEventRecord record : records) {
			try {
				byte[] dataBytes;
				if (compressionEnabled) {
					dataBytes = gzipDecompress(record.getKinesis().getData().array());
				}  else {
					dataBytes = record.getKinesis().getData().array();
				}
				HBaseWALEntry entry = mapper.readValue(dataBytes,HBaseWALEntry.class);
				for (HBaseCell c : (List<HBaseCell>)entry.getWalEdit().getCells())  {
					if (c.getType().toLowerCase().compareTo("put")==0) {
						try {
							float score = validator.getScore(c);
							writeRecord(c.getRow(),DEFAULT_HBASE_ENRICH_CULUMNFAMILY.getBytes(),"score".getBytes(), new Float(score).toString().getBytes());
							logger.log(new String(c.getRow())+ " speed:" + new String(c.getValue()) + " score: "+ score );
							
						} catch (Exception e) {
							// We are ignoring invalid records.
							logger.log("InvalidRecord: " + e.getMessage());
						}
					} else {
						logger.log("InvalidRecord: " + new String (dataBytes));
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.log(e.getMessage() + " " + e.getStackTrace());
			}
		}
		return response;
	}
	
	/**
	 * Decompress byte from Gzip
	 * 
	 * @param bytes	bytes to decompress
	 * @return 	returns bytes
	 * @throws IOException
	 */
	private byte[] gzipDecompress(byte[] bytes) throws IOException {
		ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
		GZIPInputStream zipStream = new GZIPInputStream(byteStream);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		byte[] buffer = new byte[1024];
		int len;
		while ((len = zipStream.read(buffer)) > 0) {
		    out.write(buffer, 0, len);
		}
		zipStream.close();
		out.close();
		
		return out.toByteArray();
	}
	
	public void writeRecord(byte[] rowKey,byte[] cFamily, byte[] qualifier, byte[] value ) throws Exception {
			Put p = new Put(rowKey);
			p.addColumn(cFamily, qualifier, value);
			table.put(p);
	}
	
	private String getMasterIntanceGroupId(AmazonElasticMapReduce emr, String clusterid) {
		ListInstanceGroupsRequest listInstanceGroupsRequest = new ListInstanceGroupsRequest();
		listInstanceGroupsRequest.setClusterId(clusterid);
		List<InstanceGroup> instanceGroups = emr.listInstanceGroups(listInstanceGroupsRequest).getInstanceGroups();
		for (InstanceGroup instanceGroup: instanceGroups) {
			if (instanceGroup.getInstanceGroupType().contentEquals(InstanceGroupType.MASTER.toString())) {
				return instanceGroup.getId();
			}
		}
		return null;
	}
	
	public String getMasterInstanceIpAddress(String clusterid, String region) {
		AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.withRegion(Regions.fromName(region))
				.build();
		
		String masterInstanceGroupId = getMasterIntanceGroupId(emr,clusterid);
		ListInstancesRequest listInstancesRequest = new ListInstancesRequest();
		listInstancesRequest.setInstanceGroupId(masterInstanceGroupId);
		listInstancesRequest.setClusterId(clusterid);
		
		List<Instance> instances = emr.listInstances(listInstancesRequest).getInstances();
		return instances.get(0).getPrivateDnsName();
	}
}