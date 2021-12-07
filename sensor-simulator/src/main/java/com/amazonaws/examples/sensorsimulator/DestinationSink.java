// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.examples.sensorsimulator;

import com.amazonaws.examples.sensorsimulator.model.Row;

public interface DestinationSink {
	public boolean write(Row record);
}
