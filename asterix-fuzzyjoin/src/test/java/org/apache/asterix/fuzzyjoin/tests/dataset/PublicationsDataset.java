/**
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

package org.apache.asterix.fuzzyjoin.tests.dataset;

import java.util.NoSuchElementException;

public class PublicationsDataset extends AbstractTokenizableDataset {
    protected final String name;
    protected final String path;
    protected final int noRecords;
    protected final float threshold;
    protected final String recordData;
    protected final String rSuffix, sSuffix;

    public PublicationsDataset(String name, int noRecords, float threshold, String recordData, String rSuffix,
            String sSuffix) {
        this.name = name;
        this.noRecords = noRecords;
        this.threshold = threshold;
        this.recordData = recordData;
        this.rSuffix = rSuffix;
        this.sSuffix = sSuffix;

        path = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getNoRecords() {
        return noRecords;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public String getRecordData() {
        return recordData;
    }

    @Override
    public String getSuffix(Relation relation) {
        switch (relation) {
            case R:
                return rSuffix;
            case S:
                return sSuffix;
            default:
                throw new NoSuchElementException();
        }
    }

    @Override
    public float getThreshold() {
        return threshold;
    }
}
