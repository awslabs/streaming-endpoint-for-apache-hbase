// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class UUIDHelperTest {
	@Before
	public void setUp() throws Exception {
	
	}
	private boolean isUUID(String string) {
	      try {
	          UUID.fromString(string);
	          return true;
	       } catch (Exception ex) {
	          return false;
	       }
	    }
	@Test
	public void testUUID() {
		assertTrue(isUUID(UUIDHelper.getUUID().toString()));
	}
	
}
