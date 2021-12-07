// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.lambda.exceptions;

public class InvalidInAppRecordException extends Exception {

	public InvalidInAppRecordException(String string) {
		super(string);
	}
	
	public InvalidInAppRecordException() {
		super();
	}

	private static final long serialVersionUID = 4028475492090622337L;

}
