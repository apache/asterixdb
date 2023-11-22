/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.metadata.dataset;

import java.io.Serializable;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetFormat;

public class DatasetFormatInfo implements Serializable {
    private static final long serialVersionUID = 7656132322813253435L;
    /**
     * System's default format for non-{@link DatasetConfig.DatasetType#INTERNAL} datasets
     */
    public static final DatasetFormatInfo SYSTEM_DEFAULT = new DatasetFormatInfo();
    private final DatasetFormat format;
    private final int maxTupleCount;
    private final double freeSpaceTolerance;
    private final int maxLeafNodeSize;

    private DatasetFormatInfo() {
        this(DatasetFormat.ROW, -1, 0.0d, 0);
    }

    public DatasetFormatInfo(DatasetFormat format, int maxTupleCount, double freeSpaceTolerance, int maxLeafNodeSize) {
        this.format = format;
        this.maxTupleCount = maxTupleCount;
        this.freeSpaceTolerance = freeSpaceTolerance;
        this.maxLeafNodeSize = maxLeafNodeSize;
    }

    public DatasetFormat getFormat() {
        return format;
    }

    public int getMaxTupleCount() {
        return maxTupleCount;
    }

    public double getFreeSpaceTolerance() {
        return freeSpaceTolerance;
    }

    public int getMaxLeafNodeSize() {
        return maxLeafNodeSize;
    }

    @Override
    public String toString() {
        return "(format:" + format + ", maxTupleCount:" + maxTupleCount + ')';
    }
}
