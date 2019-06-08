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
package org.apache.asterix.common.storage;

import java.io.Serializable;
import java.util.Objects;

public class DatasetCopyIdentifier implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String dataset;
    private final String dataverse;
    private final String rebalance;

    private DatasetCopyIdentifier(String dataverse, String datasetName, String rebalance) {
        this.dataverse = dataverse;
        this.dataset = datasetName;
        this.rebalance = rebalance;
    }

    public static DatasetCopyIdentifier of(String dataverse, String datasetName, String rebalance) {
        return new DatasetCopyIdentifier(dataverse, datasetName, rebalance);
    }

    public String getDataset() {
        return dataset;
    }

    public String getRebalance() {
        return rebalance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatasetCopyIdentifier that = (DatasetCopyIdentifier) o;
        return Objects.equals(dataverse, that.dataverse) && Objects.equals(dataset, that.dataset)
                && Objects.equals(rebalance, that.rebalance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverse, dataset, rebalance);
    }

    public String getDataverse() {
        return dataverse;
    }

    public boolean isMatch(ResourceReference resourceReference) {
        return resourceReference.getDataverse().equals(dataverse) && resourceReference.getDataset().equals(dataset)
                && resourceReference.getRebalance().equals(rebalance);
    }

    @Override
    public String toString() {
        return "DatasetCopyIdentifier{" + "dataset='" + dataset + '\'' + ", dataverse='" + dataverse + '\''
                + ", rebalance='" + rebalance + '\'' + '}';
    }
}