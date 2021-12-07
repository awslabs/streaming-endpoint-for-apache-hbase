// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.datasink.model;

import java.io.IOException;
import java.util.Map;

import com.confluex.mock.http.ClientRequest;

public class KinesisAPIRequest {
		private ClientRequest clientRequest; 
		private String apiName;
		private Map<String,String> headers;
		
		public KinesisAPIRequest(ClientRequest clientRequest) throws IOException {
			this.clientRequest = clientRequest;
			this.headers = clientRequest.getHeaders();
			
			this.apiName = this.headers.get("x-amz-target");
			if ( this.apiName == null) {
				throw new IOException("unrecognized http request." + this.clientRequest);
			}
		}
	
		public String getAPIName() {
			return this.apiName;
			
		}
		
		public String getHeader(String str) {
			return this.headers.get(str);
		}
		
		public ClientRequest getClientRequest() {
			return this.clientRequest;
		}
		
		public String getBody() {
			return this.clientRequest.getBody();
		}

}
