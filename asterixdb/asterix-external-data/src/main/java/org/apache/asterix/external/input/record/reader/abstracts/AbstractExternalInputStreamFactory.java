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
package org.apache.asterix.external.input.record.reader.abstracts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.provider.context.ExternalStreamRuntimeDataContext;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public abstract class AbstractExternalInputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;

    protected Map<String, String> configuration;
    protected final List<PartitionWorkLoadBasedOnSize> partitionWorkLoadsBasedOnSize = new ArrayList<>();
    protected IExternalFilterEvaluatorFactory filterEvaluatorFactory;
    protected transient AlgebricksAbsolutePartitionConstraint partitionConstraint;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.STREAM;
    }

    @Override
    public abstract AsterixInputStream createInputStream(IExternalDataRuntimeContext context)
            throws HyracksDataException;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    protected int getPartitionsCount() {
        return getPartitionConstraint().getLocations().length;
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws AlgebricksException, HyracksDataException {
        this.configuration = configuration;
        this.partitionConstraint = ((ICcApplicationContext) ctx.getApplicationContext()).getClusterStateManager()
                .getSortedClusterLocations();
        this.filterEvaluatorFactory = filterEvaluatorFactory;
    }

    @Override
    public IExternalDataRuntimeContext createExternalDataRuntimeContext(IHyracksTaskContext context, int partition) {
        IExternalFilterValueEmbedder valueEmbedder =
                filterEvaluatorFactory.createValueEmbedder(context.getWarningCollector());
        return new ExternalStreamRuntimeDataContext(context, partition, valueEmbedder);
    }

    public static class PartitionWorkLoadBasedOnSize implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<String> filePaths = new ArrayList<>();
        private long totalSize = 0;

        public PartitionWorkLoadBasedOnSize() {
        }

        public List<String> getFilePaths() {
            return filePaths;
        }

        public void addFilePath(String filePath, long size) {
            this.filePaths.add(filePath);
            this.totalSize += size;
        }

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return "Files: " + filePaths.size() + ", Total Size: " + totalSize;
        }
    }

    public static class IncludeExcludeMatcher implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<Matcher> matchersList;
        private final BiPredicate<List<Matcher>, String> predicate;

        public IncludeExcludeMatcher(List<Matcher> matchersList, BiPredicate<List<Matcher>, String> predicate) {
            this.matchersList = matchersList;
            this.predicate = predicate;
        }

        public List<Matcher> getMatchersList() {
            return matchersList;
        }

        public BiPredicate<List<Matcher>, String> getPredicate() {
            return predicate;
        }
    }
}
