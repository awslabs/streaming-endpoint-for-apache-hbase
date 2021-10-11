// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsemulator;

import com.amazonaws.examples.sensorsemulator.model.Row;

public interface DestinationSink {
	public boolean write(Row record);
}
