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

package org.apache.asterix.runtime.evaluators.functions;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.FullTextContainsFunctionEvaluator;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;

// Descriptor for the ftcontains() function.
//
// This is different from the full-text config descriptor (FullTextConfigDescriptor)
// which contains a tokenizer and full-text filter descriptors (AbstractFullTextFilterDescriptor).
@MissingNullInOutFunction
public class FullTextContainsFunctionDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 2L;

    // parameter name and its type - based on the order of parameters in this map, parameters will be re-arranged.
    private static final Map<String, ATypeTag> paramTypeMap = new LinkedHashMap<>();

    public static final String SEARCH_MODE_OPTION = "mode";
    private static final byte[] SEARCH_MODE_OPTION_ARRAY = UTF8StringUtil.writeStringToBytes(SEARCH_MODE_OPTION);
    private static final byte[] DISJUNCTIVE_SEARCH_MODE_OPTION_ARRAY =
            UTF8StringUtil.writeStringToBytes(SearchMode.ANY.getValue());
    private static final byte[] CONJUNCTIVE_SEARCH_MODE_OPTION_ARRAY =
            UTF8StringUtil.writeStringToBytes(SearchMode.ALL.getValue());

    public enum SearchMode {
        ANY("any"),
        ALL("all");

        private String value;

        SearchMode(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static final String FULLTEXT_CONFIG_OPTION = "config";
    private IFullTextConfigEvaluatorFactory fullTextConfigEvaluatorFactory;

    static {
        paramTypeMap.put(SEARCH_MODE_OPTION, ATypeTag.STRING);
        paramTypeMap.put(FULLTEXT_CONFIG_OPTION, ATypeTag.STRING);
    }

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new FullTextContainsFunctionDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new FunctionTypeInferers.FullTextContainsTypeInferer();
        }
    };

    public FullTextContainsFunctionDescriptor() {
    }

    @Override
    public void setImmutableStates(Object... states) {
        super.setImmutableStates(states);

        IFullTextConfigEvaluatorFactory fullTextConfigEvaluatorFactory = (IFullTextConfigEvaluatorFactory) states[0];
        this.fullTextConfigEvaluatorFactory = fullTextConfigEvaluatorFactory;
    }

    /**
     * Creates full-text search evaluator. There are three arguments:
     * arg0: Expression1 - search field
     * arg1: Expression2 - search predicate
     * arg2 and so on: Full-text search option
     */
    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 2L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new FullTextContainsFunctionEvaluator(args, ctx, fullTextConfigEvaluatorFactory);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.FULLTEXT_CONTAINS;
    }

    public static byte[] getSearchModeOptionArray() {
        return SEARCH_MODE_OPTION_ARRAY;
    }

    public static byte[] getDisjunctiveFTSearchOptionArray() {
        return DISJUNCTIVE_SEARCH_MODE_OPTION_ARRAY;
    }

    public static byte[] getConjunctiveFTSearchOptionArray() {
        return CONJUNCTIVE_SEARCH_MODE_OPTION_ARRAY;
    }

    public static Map<String, ATypeTag> getParamTypeMap() {
        return paramTypeMap;
    }
}
