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

import static org.apache.asterix.external.util.ExternalDataConstants.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
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
    protected transient AlgebricksAbsolutePartitionConstraint partitionConstraint;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.STREAM;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public abstract AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    @Override
    public abstract void configure(IServiceContext ctx, Map<String, String> configuration,
            IWarningCollector warningCollector) throws AlgebricksException;

    /**
     * Finds the smallest workload and returns it
     *
     * @return the smallest workload
     */
    protected PartitionWorkLoadBasedOnSize getSmallestWorkLoad() {
        PartitionWorkLoadBasedOnSize smallest = partitionWorkLoadsBasedOnSize.get(0);
        for (PartitionWorkLoadBasedOnSize partition : partitionWorkLoadsBasedOnSize) {
            // If the current total size is 0, add the file directly as this is a first time partition
            if (partition.getTotalSize() == 0) {
                smallest = partition;
                break;
            }
            if (partition.getTotalSize() < smallest.getTotalSize()) {
                smallest = partition;
            }
        }

        return smallest;
    }

    protected IncludeExcludeMatcher getIncludeExcludeMatchers() throws CompilationException {
        // Get and compile the patterns for include/exclude if provided
        List<Matcher> includeMatchers = new ArrayList<>();
        List<Matcher> excludeMatchers = new ArrayList<>();
        String pattern = null;
        try {
            for (Map.Entry<String, String> entry : configuration.entrySet()) {
                if (entry.getKey().startsWith(KEY_INCLUDE)) {
                    pattern = entry.getValue();
                    includeMatchers.add(Pattern.compile(ExternalDataUtils.patternToRegex(pattern)).matcher(""));
                } else if (entry.getKey().startsWith(KEY_EXCLUDE)) {
                    pattern = entry.getValue();
                    excludeMatchers.add(Pattern.compile(ExternalDataUtils.patternToRegex(pattern)).matcher(""));
                }
            }
        } catch (PatternSyntaxException ex) {
            throw new CompilationException(ErrorCode.INVALID_REGEX_PATTERN, pattern);
        }

        IncludeExcludeMatcher includeExcludeMatcher;
        if (!includeMatchers.isEmpty()) {
            includeExcludeMatcher = new IncludeExcludeMatcher(includeMatchers,
                    (matchers1, key) -> ExternalDataUtils.matchPatterns(matchers1, key));
        } else if (!excludeMatchers.isEmpty()) {
            includeExcludeMatcher = new IncludeExcludeMatcher(excludeMatchers,
                    (matchers1, key) -> !ExternalDataUtils.matchPatterns(matchers1, key));
        } else {
            includeExcludeMatcher = new IncludeExcludeMatcher(Collections.emptyList(), (matchers1, key) -> true);
        }

        return includeExcludeMatcher;
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
