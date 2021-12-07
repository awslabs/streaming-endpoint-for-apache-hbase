// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

import org.apache.hadoop.conf.Configuration;

public class DataSinkFactory {
	
	public static class Builder {
		private DataSink dataSink = null;
		private Configuration config = new Configuration();
;
		
		public  DataSink build() throws Exception {
			ConfigurationUtil confUtil = new ConfigurationUtil(config);
			ClassLoader cLoader = this.getClass().getClassLoader();
			
			Class<?> cls;
			try {
			    Class.forName(confUtil.getSinkImplementationFacotry());
			    cls = cLoader.loadClass(confUtil.getSinkImplementationFacotry());
			    this.dataSink = (DataSink)cls.getDeclaredConstructor(Configuration.class).newInstance(config);
				return dataSink;
			} catch(ClassNotFoundException e) {
				    throw e;
		    }
		}

		public Builder withConfiguration(Configuration config) {
			this.config = config;
			return this;
		}
	}
	
	private  DataSinkFactory() {
	}
	
}
