
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.asterix.runtime;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionCollection;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This test goes through all the functions that should respect the "missing/null in -> missing/null out" behavior,
 * passes all possible combinations of ATypeType arguments and ensures the thrown Exceptions hae the appropriate error
 * codes.
 */

@RunWith(Parameterized.class)
public class ExceptionIT {

    // This will temporarily hold all the functions that need to be tested (codegen functions) until
    // the alternative solution is implemented
    private static Set<FunctionInfo> includedFunctions = new HashSet<>();

    @Parameterized.Parameters(name = "NullMissingTest {index}: {0}")
    public static Collection<Object[]> tests() {
        List<Object[]> tests = new ArrayList<>();

        // Get all the functions
        List<IFunctionDescriptorFactory> functions =
                FunctionCollection.createDefaultFunctionCollection().getFunctionDescriptorFactories();

        // Build the tests
        for (IFunctionDescriptorFactory func : functions) {
            String className = func.getClass().getName();
            // We test all functions except record and cast functions, which requires type settings (we test them
            // in runtime tests).
            if (!className.contains("record") && !className.contains("Cast")) {
                tests.add(new Object[] { getTestName(func), func });
            }
        }
        return tests;
    }

    private static String getTestName(IFunctionDescriptorFactory func) {
        if (func.getClass().getEnclosingClass() != null) {
            return func.getClass().getEnclosingClass().getSimpleName();
        } else if (func.getClass().getSimpleName().contains("$$Lambda")) {
            return func.getClass().getSimpleName().replaceAll("\\$\\$Lambda.*", "");
        } else {
            return func.getClass().getSimpleName();
        }
    }

    @Parameterized.Parameter
    public String testName;

    @Parameterized.Parameter(1)
    public IFunctionDescriptorFactory funcFactory;

    @Test
    public void test() throws Exception {
        IFunctionDescriptor functionDescriptor = funcFactory.createFunctionDescriptor();

        // This checks whether the current function should go through this test or get excluded
        if (!(functionDescriptor instanceof AbstractScalarFunctionDynamicDescriptor)
                || !includedFunctions.contains(new FunctionInfo(functionDescriptor.getIdentifier(), true))) {
            return;
        }

        // Get the arguments combinations
        AbstractScalarFunctionDynamicDescriptor funcDesc = (AbstractScalarFunctionDynamicDescriptor) functionDescriptor;
        int inputArity = funcDesc.getIdentifier().getArity();
        Iterator<IScalarEvaluatorFactory[]> argEvalFactoryIterator = getArgCombinations(inputArity);

        // Test is happening here
        while (argEvalFactoryIterator.hasNext()) {
            IScalarEvaluatorFactory[] factories = argEvalFactoryIterator.next();

            // Evaluate
            IScalarEvaluatorFactory evalFactory = funcDesc.createEvaluatorFactory(factories);
            IHyracksTaskContext ctx = mock(IHyracksTaskContext.class);
            try {
                IScalarEvaluator evaluator = evalFactory.createScalarEvaluator(ctx);
                IPointable resultPointable = new VoidPointable();
                evaluator.evaluate(null, resultPointable);
            } catch (Throwable e) {
                String msg = e.getMessage();
                if (msg == null) {
                    continue;
                }
                if (msg.startsWith("ASX")) {
                    // Verifies the error code.
                    int errorCode = Integer.parseInt(msg.substring(3, 7));
                    Assert.assertTrue(errorCode >= 0 && errorCode < 1000);
                } else if (msg.startsWith("HYR")) {
                    // Verifies the error code.
                    int errorCode = Integer.parseInt(msg.substring(3, 7));
                    Assert.assertTrue(errorCode >= 0 && errorCode < 1000);
                } else {
                    // Any root-level data exceptions thrown from runtime functions should have an error code.
                    Assert.assertTrue(!(e instanceof HyracksDataException) || (e.getCause() != null));
                }
            }
        }
    }

    private Iterator<IScalarEvaluatorFactory[]> getArgCombinations(final int inputArity) {
        final int argSize = inputArity >= 0 ? inputArity : 3;
        final int numCombinations = (int) Math.pow(ATypeTag.values().length, argSize);
        return new Iterator<IScalarEvaluatorFactory[]>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < numCombinations;
            }

            @Override
            public IScalarEvaluatorFactory[] next() {
                IScalarEvaluatorFactory[] scalarEvaluatorFactories = new IScalarEvaluatorFactory[argSize];
                for (int j = 0; j < argSize; ++j) {
                    int base = (int) Math.pow(ATypeTag.values().length, j);
                    // Enumerates through all possible type tags.
                    byte serializedTypeTag = (byte) ((index / base) % ATypeTag.values().length);
                    scalarEvaluatorFactories[j] = new ConstantEvalFactory(new byte[] { serializedTypeTag });
                }
                ++index;
                return scalarEvaluatorFactories;
            }
        };
    }

    // Adds all the functions that need to be tested, this is a temporary solution and will be replaced
    // once the alternative solution is implemented
    @BeforeClass
    public static void buildFunctions() {
        // Included
        // FuzzyJoinFunctionRegistrant class
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PREFIX_LEN_JACCARD, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.WORD_TOKENS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.HASHED_WORD_TOKENS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.COUNTHASHED_WORD_TOKENS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GRAM_TOKENS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.HASHED_GRAM_TOKENS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.COUNTHASHED_GRAM_TOKENS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.EDIT_DISTANCE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.EDIT_DISTANCE_CHECK, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.EDIT_DISTANCE_CONTAINS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SIMILARITY_JACCARD, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SIMILARITY_JACCARD_CHECK, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SIMILARITY_JACCARD_SORTED, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SIMILARITY_JACCARD_SORTED_CHECK, true));

        // FunctionCollection class
        // Array functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_POSITION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_REPEAT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_CONTAINS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_REVERSE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_SORT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_DISTINCT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_UNION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_INTERSECT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_IFNULL, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_CONCAT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_RANGE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_FLATTEN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_SLICE_WITH_END_POSITION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_SLICE_WITHOUT_END_POSITION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_SYMDIFF, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_SYMDIFFN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ARRAY_STAR, true));

        // Element accessors
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FIELD_ACCESS_NESTED, true));

        // Numeric functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_UNARY_MINUS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ADD, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_DIVIDE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_DIV, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_MULTIPLY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_SUBTRACT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_MOD, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_POWER, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NOT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.LEN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ABS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_CEILING, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_FLOOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ROUND, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ROUND_HALF_TO_EVEN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ROUND_HALF_TO_EVEN2, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ACOS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ASIN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ATAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_DEGREES, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_RADIANS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_COS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_COSH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_SIN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_SINH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_TAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_TANH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_EXP, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_LN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_LOG, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_SQRT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_SIGN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_TRUNC, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NUMERIC_ATAN2, true));

        // Comparisons functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.EQ, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.LT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.LE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NEQ, true));

        // If-Equals functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.MISSING_IF, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NULL_IF, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NAN_IF, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.POSINF_IF, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.NEGINF_IF, true));

        // Binary functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.BINARY_LENGTH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PARSE_BINARY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PRINT_BINARY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.BINARY_CONCAT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SUBBINARY_FROM, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SUBBINARY_FROM_TO, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FIND_BINARY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FIND_BINARY_FROM, true));

        // String functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_LIKE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_CONTAINS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_ENDS_WITH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_STARTS_WITH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SUBSTRING, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_EQUAL, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_LOWERCASE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_UPPERCASE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_LENGTH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SUBSTRING2, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SUBSTRING_BEFORE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SUBSTRING_AFTER, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_TO_CODEPOINT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CODEPOINT_TO_STRING, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_CONCAT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_JOIN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_MATCHES, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_MATCHES_WITH_FLAG, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REGEXP_LIKE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REGEXP_LIKE_WITH_FLAG, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REGEXP_POSITION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REGEXP_POSITION_WITH_FLAG, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REGEXP_REPLACE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REGEXP_REPLACE_WITH_FLAG, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_INITCAP, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_TRIM, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_LTRIM, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_RTRIM, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_TRIM2, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_LTRIM2, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_RTRIM2, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_POSITION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REPEAT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REPLACE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REPLACE_WITH_LIMIT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_REVERSE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_SPLIT, true));

        // Constructors
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.BOOLEAN_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.BINARY_HEX_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.BINARY_BASE64_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.STRING_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INT8_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INT16_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INT32_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INT64_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FLOAT_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DOUBLE_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.POINT_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.POINT3D_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.LINE_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.POLYGON_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CIRCLE_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECTANGLE_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TIME_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATE_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATETIME_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DURATION_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.YEAR_MONTH_DURATION_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DAY_TIME_DURATION_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.UUID_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_CONSTRUCTOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_CONSTRUCTOR_START_FROM_DATE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_CONSTRUCTOR_START_FROM_TIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_CONSTRUCTOR_START_FROM_DATETIME, true));

        // Spatial
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CREATE_POINT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CREATE_LINE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CREATE_POLYGON, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CREATE_CIRCLE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CREATE_RECTANGLE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SPATIAL_AREA, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SPATIAL_DISTANCE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CREATE_MBR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.SPATIAL_CELL, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_POINT_X_COORDINATE_ACCESSOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_POINT_Y_COORDINATE_ACCESSOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_CIRCLE_RADIUS_ACCESSOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_CIRCLE_CENTER_ACCESSOR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR, true));

        // Full-text function
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FULLTEXT_CONTAINS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION, true));

        // Record functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_RECORD_FIELDS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_RECORD_FIELD_VALUE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DEEP_EQUAL, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_MERGE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ADD_FIELDS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.REMOVE_FIELDS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_LENGTH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_NAMES, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_REMOVE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_RENAME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_UNWRAP, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_VALUES, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PAIRS, true));

        // Spatial and temporal type accessors
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_YEAR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_MONTH, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_DAY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_HOUR, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_MIN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_SEC, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_MILLISEC, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START_DATE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END_DATE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START_TIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END_TIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START_DATETIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END_DATETIME, true));

        // Temporal functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.UNIX_TIME_FROM_DATE_IN_DAYS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.UNIX_TIME_FROM_TIME_IN_MS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.UNIX_TIME_FROM_DATETIME_IN_MS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.UNIX_TIME_FROM_DATETIME_IN_SECS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATE_FROM_UNIX_TIME_IN_DAYS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATE_FROM_DATETIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TIME_FROM_UNIX_TIME_IN_MS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TIME_FROM_DATETIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATETIME_FROM_UNIX_TIME_IN_MS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATETIME_FROM_UNIX_TIME_IN_SECS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DATETIME_FROM_DATE_TIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CALENDAR_DURATION_FROM_DATETIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CALENDAR_DURATION_FROM_DATE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ADJUST_DATETIME_FOR_TIMEZONE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.ADJUST_TIME_FOR_TIMEZONE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_BEFORE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_AFTER, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_MEETS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_MET_BY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_OVERLAPS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_OVERLAPPED_BY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_OVERLAPPING, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_STARTS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_STARTED_BY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_COVERS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_COVERED_BY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_ENDS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_ENDED_BY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DURATION_FROM_MILLISECONDS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DURATION_FROM_MONTHS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.YEAR_MONTH_DURATION_GREATER_THAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.YEAR_MONTH_DURATION_LESS_THAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DAY_TIME_DURATION_GREATER_THAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DAY_TIME_DURATION_LESS_THAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.MONTHS_FROM_YEAR_MONTH_DURATION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.MILLISECONDS_FROM_DAY_TIME_DURATION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DURATION_EQUAL, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_YEAR_MONTH_DURATION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_DAY_TIME_DURATION, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.INTERVAL_BIN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.OVERLAP_BINS, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DAY_OF_WEEK, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PARSE_DATE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PARSE_TIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PARSE_DATETIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PRINT_DATE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PRINT_TIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.PRINT_DATETIME, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.GET_OVERLAPPING_INTERVAL, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.DURATION_FROM_INTERVAL, true));

        // Type functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.IS_ARRAY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.IS_ATOMIC, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.IS_BOOLEAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.IS_NUMBER, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.IS_OBJECT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.IS_STRING, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_ARRAY, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_ATOMIC, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_BIGINT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_BOOLEAN, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_DOUBLE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_NUMBER, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_OBJECT, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TO_STRING, true));

        includedFunctions.add(new FunctionInfo(BuiltinFunctions.TREAT_AS_INTEGER, true));

        // Cast function
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CAST_TYPE, true));
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.CAST_TYPE_LAX, true));

        // Record function
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RECORD_PAIRS, true));

        // Other functions
        includedFunctions.add(new FunctionInfo(BuiltinFunctions.RANDOM_WITH_SEED, true));
    }

}
