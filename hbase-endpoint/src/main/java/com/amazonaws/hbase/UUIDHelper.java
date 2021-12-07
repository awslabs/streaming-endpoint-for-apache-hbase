// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

public class UUIDHelper {
	public static byte[] getBytesFromUUID(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());

        return bb.array();
    }

    public static UUID getUUIDFromBytes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        Long high = byteBuffer.getLong();
        Long low = byteBuffer.getLong();

        return new UUID(high, low);
    }
    
    public static UUID getUUID() {
    	return UUID.randomUUID();
    }
    
    public static String getBase64UUID() {
    	String encodedString = Base64.getEncoder().encodeToString(getBytesFromUUID(getUUID()));
    	return encodedString;
    }
}
