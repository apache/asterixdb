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
package org.apache.asterix.common.config;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowPOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.control.common.config.OptionTypes;

public class OptimizationConfUtil {

    private static final int MIN_FRAME_LIMIT_FOR_SORT = AbstractStableSortPOperator.MIN_FRAME_LIMIT_FOR_SORT;
    private static final int MIN_FRAME_LIMIT_FOR_GROUP_BY = AbstractGroupByPOperator.MIN_FRAME_LIMIT_FOR_GROUP_BY;
    private static final int MIN_FRAME_LIMIT_FOR_JOIN = AbstractJoinPOperator.MIN_FRAME_LIMIT_FOR_JOIN;
    private static final int MIN_FRAME_LIMIT_FOR_WINDOW = WindowPOperator.MIN_FRAME_LIMIT_FOR_WINDOW;
    public static final int MIN_FRAME_LIMIT_FOR_TEXT_SEARCH = 5; // see InvertedIndexPOperator

    private OptimizationConfUtil() {
    }

    public static PhysicalOptimizationConfig createPhysicalOptimizationConf(CompilerProperties compilerProperties,
            Map<String, Object> querySpecificConfig, SourceLocation sourceLoc) throws AlgebricksException {
        int frameSize = compilerProperties.getFrameSize();
        int sortFrameLimit = getSortNumFrames(compilerProperties, querySpecificConfig, sourceLoc);
        int groupFrameLimit = getFrameLimit(CompilerProperties.COMPILER_GROUPMEMORY_KEY,
                (String) querySpecificConfig.get(CompilerProperties.COMPILER_GROUPMEMORY_KEY),
                compilerProperties.getGroupMemorySize(), frameSize, MIN_FRAME_LIMIT_FOR_GROUP_BY, sourceLoc);
        int joinFrameLimit = getFrameLimit(CompilerProperties.COMPILER_JOINMEMORY_KEY,
                (String) querySpecificConfig.get(CompilerProperties.COMPILER_JOINMEMORY_KEY),
                compilerProperties.getJoinMemorySize(), frameSize, MIN_FRAME_LIMIT_FOR_JOIN, sourceLoc);
        int windowFrameLimit = getFrameLimit(CompilerProperties.COMPILER_WINDOWMEMORY_KEY,
                (String) querySpecificConfig.get(CompilerProperties.COMPILER_WINDOWMEMORY_KEY),
                compilerProperties.getWindowMemorySize(), frameSize, MIN_FRAME_LIMIT_FOR_WINDOW, sourceLoc);
        int textSearchFrameLimit = getTextSearchNumFrames(compilerProperties, querySpecificConfig, sourceLoc);
        int sortNumSamples = getSortSamples(compilerProperties, querySpecificConfig, sourceLoc);
        boolean fullParallelSort = getSortParallel(compilerProperties, querySpecificConfig);

        PhysicalOptimizationConfig physOptConf = new PhysicalOptimizationConfig();
        physOptConf.setFrameSize(frameSize);
        physOptConf.setMaxFramesExternalSort(sortFrameLimit);
        physOptConf.setMaxFramesExternalGroupBy(groupFrameLimit);
        physOptConf.setMaxFramesForJoin(joinFrameLimit);
        physOptConf.setMaxFramesForWindow(windowFrameLimit);
        physOptConf.setMaxFramesForTextSearch(textSearchFrameLimit);
        physOptConf.setSortParallel(fullParallelSort);
        physOptConf.setSortSamples(sortNumSamples);

        return physOptConf;
    }

    public static int getSortNumFrames(CompilerProperties compilerProperties, Map<String, Object> querySpecificConfig,
            SourceLocation sourceLoc) throws AlgebricksException {
        return getFrameLimit(CompilerProperties.COMPILER_SORTMEMORY_KEY,
                (String) querySpecificConfig.get(CompilerProperties.COMPILER_SORTMEMORY_KEY),
                compilerProperties.getSortMemorySize(), compilerProperties.getFrameSize(), MIN_FRAME_LIMIT_FOR_SORT,
                sourceLoc);
    }

    public static int getTextSearchNumFrames(CompilerProperties compilerProperties,
            Map<String, Object> querySpecificConfig, SourceLocation sourceLoc) throws AlgebricksException {
        return getFrameLimit(CompilerProperties.COMPILER_TEXTSEARCHMEMORY_KEY,
                (String) querySpecificConfig.get(CompilerProperties.COMPILER_TEXTSEARCHMEMORY_KEY),
                compilerProperties.getTextSearchMemorySize(), compilerProperties.getFrameSize(),
                MIN_FRAME_LIMIT_FOR_TEXT_SEARCH, sourceLoc);
    }

    @SuppressWarnings("squid:S1166") // Either log or rethrow this exception
    private static int getFrameLimit(String parameterName, String parameter, long memBudgetInConfiguration,
            int frameSize, int minFrameLimit, SourceLocation sourceLoc) throws AlgebricksException {
        IOptionType<Long> longBytePropertyInterpreter = OptionTypes.LONG_BYTE_UNIT;
        long memBudget;
        try {
            memBudget = parameter == null ? memBudgetInConfiguration : longBytePropertyInterpreter.parse(parameter);
        } catch (IllegalArgumentException e) {
            throw AsterixException.create(ErrorCode.COMPILATION_ERROR, sourceLoc, e.getMessage());
        }
        int frameLimit = (int) (memBudget / frameSize);
        if (frameLimit < minFrameLimit) {
            throw AsterixException.create(ErrorCode.COMPILATION_BAD_QUERY_PARAMETER_VALUE, sourceLoc, parameterName,
                    frameSize * minFrameLimit, "bytes");
        }
        // sets the frame limit to the minimum frame limit if the calculated frame limit is too small.
        return Math.max(frameLimit, minFrameLimit);
    }

    private static boolean getSortParallel(CompilerProperties compilerProperties,
            Map<String, Object> querySpecificConfig) {
        String valueInQuery = (String) querySpecificConfig.get(CompilerProperties.COMPILER_SORT_PARALLEL_KEY);
        if (valueInQuery != null) {
            return OptionTypes.BOOLEAN.parse(valueInQuery);
        }
        return compilerProperties.getSortParallel();
    }

    @SuppressWarnings("squid:S1166") // Either log or rethrow this exception
    private static int getSortSamples(CompilerProperties compilerProperties, Map<String, Object> querySpecificConfig,
            SourceLocation sourceLoc) throws AsterixException {
        String valueInQuery = (String) querySpecificConfig.get(CompilerProperties.COMPILER_SORT_SAMPLES_KEY);
        try {
            return valueInQuery == null ? compilerProperties.getSortSamples()
                    : OptionTypes.POSITIVE_INTEGER.parse(valueInQuery);
        } catch (IllegalArgumentException e) {
            throw AsterixException.create(ErrorCode.COMPILATION_BAD_QUERY_PARAMETER_VALUE, sourceLoc,
                    CompilerProperties.COMPILER_SORT_SAMPLES_KEY, 1, "samples");
        }
    }
}
