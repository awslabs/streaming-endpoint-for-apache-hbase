// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.confluex.mock.http.matchers.HttpMatchers.anyRequest;
import static org.junit.Assert.assertEquals;
import static com.confluex.mock.http.matchers.HttpMatchers.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.amazonaws.hbase.datasink.model.KinesisAPIRequest;
import com.amazonaws.hbase.datasink.model.KinesisPutRecordsRequest;
import com.amazonaws.hbase.datasink.model.KinesisPutRecordsResponse;

import com.amazonaws.hbase.model.HBaseWALEntry;
import com.amazonaws.hbase.StreamingReplicationEndpoint;
import com.amazonaws.hbase.model.HBaseCell;

import com.amazonaws.hbase.datasink.model.RecordResult;
import com.confluex.mock.http.ClientRequest;
import com.confluex.mock.http.MockHttpsServer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import groovy.lang.Closure;
import com.amazonaws.hbase.datasink.model.Record;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Set;
import java.util.zip.GZIPInputStream;


public class KinesisReplicationEndpointTest {
	private static final Logger LOG = LoggerFactory.getLogger(KinesisReplicationEndpointTest.class);    
    private HBaseTestingUtility UTIL;
  
    private static final String PEER_NAME = "kinesisreplicationendpoint";
    private static final String STREAM_NAME = "stream";
    private static final TableName TABLE_NAME = TableName.valueOf("table");
    private static final String ROWKEY = "row";
    private static final byte[] ROWKEY_BYTES = Bytes.toBytes(ROWKEY);
    private static final String COLUMN_FAMILY = "columnfamily";
    private static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
    private static final String QUANTIFIER = "quantifier";
    private static final byte[] QUANTIFIER_BYTES = Bytes.toBytes(QUANTIFIER);
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = Bytes.toBytes(VALUE);
    
    private int numRegionServers;
    
	public static String listShardsResponce_body = ""
			+ "{" 
			+	"\"Shards\": "
			+	"["
			+		"{"
			+			"\"ShardId\": \"shardId-000000000000\","
			+			"\"HashKeyRange\":"
			+   		"{" 
			+				"\"StartingHashKey\": \"0\","
			+       		"\"EndingHashKey\": \"340282366920938463463374607431768211455\""
			+ 			"},"	
			+			"\"SequenceNumberRange\":"
			+			"{"
			+				"\"StartingSequenceNumber\":\"49620181809378275237997278108765058058967011852973768706\""
			+ 			"}"
			+		"}"
			+	"]"
			+ "}";


    private TableDescriptor table;
    private Connection connection;
    
    private int port;
    private MockHttpsServer server;
    
	interface KinesisResponse {
        public String response(ClientRequest request);
    }
	
    @Before
    public void setUp() throws Exception {
    	
        Configuration config = HBaseConfiguration.create();
		server = new MockHttpsServer();
		
		server.respondTo(anyRequest()).withStatus(200).withBody("{}");
		
		server.respondTo(post("/").and(header("x-amz-target","Kinesis_20131202.ListShards")))
			.withStatus(200)
			.withBody(listShardsResponce_body)
			.withHeader("Content-Type","application/x-amz-json-1.1")
			.withHeader("x-amzn-RequestId", "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa");


		server.respondTo(post("/").and(header("x-amz-target","Kinesis_20131202.PutRecords")))
			.withStatus(200)
			.withHeader("Content-Type","application/x-amz-json-1.1")
			.withHeader("x-amzn-RequestId", "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa")
			.withBody(new Closure<String>(null) {

			/**
			 * 
			 */
			
			private static final long serialVersionUID = 1L;

			@Override
            public String call(Object request) {
           		ObjectMapper objectMapper = new ObjectMapper();
        		objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        		objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
        		objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        		
        		
           		String body = ((ClientRequest)request).getBody();
           		LOG.info("******************\n" + body);
           		KinesisPutRecordsRequest obj = null;
           		try {
					obj = objectMapper.readValue(body,KinesisPutRecordsRequest.class );
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
           		KinesisPutRecordsResponse result = new KinesisPutRecordsResponse();
           		ArrayList<RecordResult> records=new ArrayList<RecordResult>();
           		for (int i =0; i< obj.getRecords().size(); i++) {
           			RecordResult t = new RecordResult();
           			t.setSequenceNumber(Integer.toString(i));
           			t.setShardId("shardId-000000000000");
           			records.add(t);
           		}
           	
           		result.setFailedRecordCount(0);
           		result.setRecords(records);
           		
           		try {
           			String t = objectMapper.writeValueAsString(result);
           			LOG.info("********************\n" + t);
					return t;
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
           		return "{ }";
            }
        });
		
		port = server.getPort();
		
        config.set("hbase.replication.kinesis.stream-table-map", TABLE_NAME.getNameAsString()+":"+STREAM_NAME);
        config.set("hbase.replication.kinesis.region","ap-southeast-2");
        config.setInt("hbase.replication.kinesis.max-connection", 1);
        config.set("hbase.replication.kinesis.endpoint", "localhost");
        config.setInt("hbase.replication.kinesis.endpoint-port", port);
        config.setBoolean("hbase.replication.kinesis.aggregation-enabled", false);        
        config.setBoolean("hbase.replication.bulkload.enabled", true);
		config.setInt("hbase.replication.kinesis.cw-endpoint-port", port);
		config.set("hbase.replication.kinesis.cw-endpoint","localhost");
		config.set("hbase.replication.kinesis.log-level","debug");
		config.set("hbase.replication.kinesis.metric-level","none");
		config.setBoolean("hbase.replication.kinesis.verify-ssl-cert",false);
		config.setInt("hbase.replication.kinesis.record-ttl", 200);
		config.set("hbase.replication.sink-factory-class","com.amazonaws.hbase.datasink.KinesisDataSinkImpl");
		config.setBoolean("hbase.replication.compression-enabled", true);
        
        config.set("hbase.replication.cluster.id", "hbase1");
        
        UTIL = new HBaseTestingUtility(config);
        UTIL.startMiniCluster();
        
        numRegionServers = UTIL.getHBaseCluster().getRegionServerThreads().size();

        
        connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
        
    }
    
	private boolean createReplictionPeer() throws IOException {
		Admin admin = UTIL.getAdmin();
		
		try {
			admin.addReplicationPeer(PEER_NAME, new ReplicationPeerConfig().setReplicationEndpointImpl("com.amazonaws.hbase.StreamingReplicationEndpoint"), true);
		} catch (Exception e) {
			LOG.error("Unable to add the replication peer. ",e);
			return false;
		}
		LOG.info("--- Added Replication Peer " + StreamingReplicationEndpoint.class.getCanonicalName() + "---");
		return true;
	}

	private boolean createTable() throws IOException {
		try {
			table = TableDescriptorBuilder.newBuilder(TABLE_NAME)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_BYTES)
                    .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build();
		
			Admin admin = UTIL.getAdmin();
			admin.createTable(table);
			UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
			LOG.info("--- created test table ---");
		} catch (Exception e) {
			LOG.error("Failed to create HBase table: " + e.getMessage());
			return false;
		}
		return true;
	}
		
	private boolean putOneRecord() throws Exception {
		Put put = new Put(ROWKEY_BYTES);
		Table table = connection.getTable(TABLE_NAME);
		
	    put.addColumn(COLUMN_FAMILY_BYTES, COLUMN_FAMILY_BYTES, VALUE_BYTES);
	    
	    table.put(put);
	    LOG.info("--- Put one record into the table for replication ---");
	    Thread.sleep(10000);
	    return true;
	}
    

	@Test
	public void testKinesisReplicationEndpoint() throws Exception {
		createTable();
		if (createReplictionPeer() == false ) {
			Assert.fail("Unable to add replication peer.");
		}
		putOneRecord();
		LOG.info("--- Replicated records ---");
		for (ClientRequest cr : server.getRequests()) {
			KinesisAPIRequest api = new KinesisAPIRequest(cr);
			LOG.info(api.getClientRequest().toString());
			if ( api.getAPIName().contains("PutRecords")) {
				KinesisPutRecordsRequest req = new ObjectMapper().readValue(api.getBody(), KinesisPutRecordsRequest.class);
				LOG.info("Stream Name: " + req.getStreamName());
				// Check we are getting request for the right stream name
				assertEquals("Checking stream name in the Kinesis request",req.getStreamName(),STREAM_NAME);
				// Check we are getting only 1 record for 1 put. More than one means HBase has retried;
				assertEquals("Checking Kinesis record count.",req.getRecords().size(),1);
				// Get the only record
				Record r = req.getRecords().get(0);
				// decode the Base64 Data to get the Json serialized record 
				String jsonData = new String(gzipUncompress(Base64.getDecoder().decode(r.getData())));
				LOG.info("Serialized HBase Put item: " + jsonData);
				// Deserialize data
			
				try {
					HBaseWALEntry entry  = (HBaseWALEntry)new ObjectMapper().readValue(jsonData, HBaseWALEntry.class);

					assertEquals("Checking the tablename ",TABLE_NAME.toString(),new String(entry.getWalKey().getTableName()));
					
					//Check families
					Set<String> families = entry.getWalEdit().getFamilies();					
					// We are expecting only 1 columnFamily.
					assertEquals("Checking ColumnFamily count",families.size(),1);
					
					HBaseCell cell = (HBaseCell) entry.getWalEdit().getCells().get(0);
					// check attributes to be as expected
					assertEquals("Checking ColumnFamily",new String(cell.getFamily()), COLUMN_FAMILY);
					assertEquals("Checking rowKey",new String(cell.getRow()),ROWKEY);
					assertEquals("Checking action",cell.getType().toLowerCase(),"put");
					assertEquals("Checking value",new String(cell.getValue()),VALUE);
					
				} catch (JsonMappingException e) {
					e.printStackTrace();
					Assert.fail("Unable to deserialize");
				} catch (JsonProcessingException e) {
					e.printStackTrace();
					Assert.fail("Unable to deserialize.");
				} 
			}
		}
		
	}
	
	/**
	 * Uncompress byte from Gzip
	 * 
	 * @param bytes	bytes to uncompress
	 * @return 	returns bytes
	 * @throws IOException
	 */
	private byte[] gzipUncompress(byte[] bytes) throws IOException {
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
	
    @After
    public void stopServer() throws Exception {
    	UTIL.shutdownMiniCluster();
    	server.stop();
    }

}
