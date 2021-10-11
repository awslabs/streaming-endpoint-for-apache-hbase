// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase.lambda;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.amazonaws.hbase.lambda.exceptions.InvalidInAppRecordException;
import com.amazonaws.hbase.model.HBaseCell;


// A simple validation and scoring class. In real-life this needs to be more sophisticated. 
public class RecordValidator {
	
	public float getScore(HBaseCell column) throws InvalidInAppRecordException {

			validateRecord(column);
			if (new String(column.getQualifier()).toLowerCase().compareTo("speed")==0) {
				float speed = Float.parseFloat(new String(column.getValue()));
				if (speed >= 40 && speed <= 120) {
					return  (1-(Math.abs(speed - 80)/80)) * 100;
				} else {
					if (speed > 120) {
						return 0; 
					} else {
						return 0; 
					} 
				}
			} else {
				// we should never be here !
				throw new InvalidInAppRecordException("not a speed record.");
			}
	}
	
	public void validateRecord(HBaseCell cell) throws InvalidInAppRecordException {
				
		byte[] cfamily = cell.getFamily();
		byte[] cqualifier =	cell.getQualifier();
		byte[] rowkey = cell.getRow();
		long ts = cell.getTimeStamp();
		String type = cell.getType();
		byte[] value = cell.getValue();
		
		switch (type.toLowerCase()) {
		case "put":
		case "deletefamily":
			// some valid operations
			break;
		default:
				throw new InvalidInAppRecordException(cell.toString());
		}
		
		if ( 
			isValidUTF8(cfamily) &&
			isValidUTF8(cqualifier) &&
			isValidUTF8(rowkey) 
			) {
			// It's fine.
				
		} else {
			throw new InvalidInAppRecordException(cell.toString());
		}
	}
	
	private boolean isValidUTF8(byte[] barr){
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
        ByteBuffer buf = ByteBuffer.wrap(barr);
        try {
            decoder.decode(buf);
        }
        catch(CharacterCodingException e){
            return false;
        }
        return true;
    }

}
