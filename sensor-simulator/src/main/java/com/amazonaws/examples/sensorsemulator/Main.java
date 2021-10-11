// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	private ExecutorService es = Executors.newCachedThreadPool();
	private HbaseSensorSink sink;
	
	
	public  Main() throws IOException {
		this.sink = new HbaseSensorSink();
	}
	
	public Main(String tableName) throws IOException {
		this.sink = new HbaseSensorSink(tableName);
	}
	public void start() throws InterruptedException {
		for (int i = 0; i < Configuration.DEFAULT_SENSOR_COUNT; i++ ){
			es.execute(new RecordProducer("sensor-"+i,Configuration.DEFAULT_SAMPLES_PERSECOND));
		}
		
		es.shutdown();
		while (! es.awaitTermination(1, TimeUnit.MINUTES));
	}
	
	public static void main(String[] args) throws IOException {
		Main main;
		if (args.length >= 1) {
			main = new Main(args[0]);
		} else {
			main = new Main();
		}
		
		try {
			main.start();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public class RecordProducer implements Runnable {
		private String name;
		private int rps;
		private Random random = new Random();
		private String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		
		public RecordProducer(String name,int records_persecond) {
			this.name = name;
			this.rps = records_persecond;

		}
		
		private String randomString(int length) {
		  
		    StringBuilder buffer = new StringBuilder(length);
		    
		    for (int i = 0; i < length; i++) {
		        int randomLimitedInt = random.nextInt(chars.length());
		        buffer.append((char) chars.charAt(randomLimitedInt));
		    }
		    String generatedString = buffer.toString();
		    
		    return generatedString;
		}
		
		private int randomNumber(int min,int max) {
			return random.nextInt(max-min) + min;
		}
		
		public void run() {
			int sleep = 1000/rps;
			while (true) {
				TrafficRecord rec = new TrafficRecord(name,System.currentTimeMillis(),randomNumber(40,120),randomString(6));
		
				sink.write(rec);
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}
	}
}
