// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.hbase;

import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.ByteBufferUtils;

public class CellUtil {
	public static byte[] cloneFamily(Cell cell) {
		byte[] output = new byte[cell.getFamilyLength()];
		copyFamilyTo(cell, output, 0);
		return output;
	}
	
	public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
		byte fLen = cell.getFamilyLength();
		if (cell instanceof ByteBufferExtendedCell) {
			ByteBufferUtils.copyFromBufferToArray(destination,
					((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
					((ByteBufferExtendedCell) cell).getFamilyPosition(), destinationOffset, fLen);
		} else {
			System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination,
					destinationOffset, fLen);
		}
		return destinationOffset + fLen;
	}

	public static byte[] cloneRow(Cell cell) {
		byte[] output = new byte[cell.getRowLength()];
		copyRowTo(cell, output, 0);
		return output;
	}

	public static int copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
		short rowLen = cell.getRowLength();
		if (cell instanceof ByteBufferExtendedCell) {
			ByteBufferUtils.copyFromBufferToArray(destination,
					((ByteBufferExtendedCell) cell).getRowByteBuffer(),
					((ByteBufferExtendedCell) cell).getRowPosition(), destinationOffset, rowLen);
		} else {
			System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset,
					rowLen);
		}
		return destinationOffset + rowLen;
	}
}
