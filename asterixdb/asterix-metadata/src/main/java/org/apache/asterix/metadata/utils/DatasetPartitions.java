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
package org.apache.asterix.metadata.utils;

import java.io.Serializable;
import java.util.List;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

public class DatasetPartitions implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Dataset dataset;
    private final List<Integer> partitions;
    private final IIndexDataflowHelperFactory primaryIndexDataflowHelperFactory;
    private final List<IIndexDataflowHelperFactory> secondaryIndexDataflowHelperFactories;

    public DatasetPartitions(Dataset dataset, List<Integer> partitions,
            IIndexDataflowHelperFactory primaryIndexDataflowHelperFactory,
            List<IIndexDataflowHelperFactory> secondaryIndexDataflowHelperFactories) {
        this.dataset = dataset;
        this.partitions = partitions;
        this.primaryIndexDataflowHelperFactory = primaryIndexDataflowHelperFactory;
        this.secondaryIndexDataflowHelperFactories = secondaryIndexDataflowHelperFactories;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public IIndexDataflowHelperFactory getPrimaryIndexDataflowHelperFactory() {
        return primaryIndexDataflowHelperFactory;
    }

    public List<IIndexDataflowHelperFactory> getSecondaryIndexDataflowHelperFactories() {
        return secondaryIndexDataflowHelperFactories;
    }

    @Override
    public String toString() {
        return "{ \"dataset\" : " + dataset + ", \"partitions\" : " + partitions + " }";
    }
}
