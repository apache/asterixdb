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
package org.apache.asterix.om.functions;

import static org.apache.asterix.om.functions.BuiltinFunctions.WindowFunctionProperty.ALLOW_FROM_FIRST_LAST;
import static org.apache.asterix.om.functions.BuiltinFunctions.WindowFunctionProperty.ALLOW_RESPECT_IGNORE_NULLS;
import static org.apache.asterix.om.functions.BuiltinFunctions.WindowFunctionProperty.HAS_LIST_ARG;
import static org.apache.asterix.om.functions.BuiltinFunctions.WindowFunctionProperty.INJECT_ORDER_ARGS;
import static org.apache.asterix.om.functions.BuiltinFunctions.WindowFunctionProperty.MATERIALIZE_PARTITION;
import static org.apache.asterix.om.functions.BuiltinFunctions.WindowFunctionProperty.NO_FRAME_CLAUSE;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABinaryTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABooleanArrayContainsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ACircleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADateTimeTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADateTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADayTimeDurationTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADurationTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AFloatTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AGeometryTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt16TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt32ArrayPositionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt32TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt8TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AIntervalTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ALineTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AListFirstTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AListMultiListArgsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AListTypeComputer;
import org.apache.asterix.om.typecomputer.impl.APoint3DTypeComputer;
import org.apache.asterix.om.typecomputer.impl.APointTypeComputer;
import org.apache.asterix.om.typecomputer.impl.APolygonTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ARectangleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AStringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ATemporalInstanceTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ATimeTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AUUIDTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AYearMonthDurationTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AnyTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ArrayExceptTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ArrayIfNullTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ArrayRangeTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ArrayRepeatTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BitMultipleValuesTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BitValuePositionFlagTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BooleanFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BooleanOnlyTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BooleanOrMissingTypeComputer;
import org.apache.asterix.om.typecomputer.impl.CastTypeComputer;
import org.apache.asterix.om.typecomputer.impl.CastTypeLaxComputer;
import org.apache.asterix.om.typecomputer.impl.ClosedRecordConstructorResultType;
import org.apache.asterix.om.typecomputer.impl.CollectionMemberResultType;
import org.apache.asterix.om.typecomputer.impl.CollectionToSequenceTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ConcatNonNullTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ConcatTypeComputer;
import org.apache.asterix.om.typecomputer.impl.DoubleIfTypeComputer;
import org.apache.asterix.om.typecomputer.impl.FieldAccessByIndexResultType;
import org.apache.asterix.om.typecomputer.impl.FieldAccessByNameResultType;
import org.apache.asterix.om.typecomputer.impl.FieldAccessNestedResultType;
import org.apache.asterix.om.typecomputer.impl.FullTextContainsResultTypeComputer;
import org.apache.asterix.om.typecomputer.impl.GetOverlappingInvervalTypeComputer;
import org.apache.asterix.om.typecomputer.impl.IfMissingOrNullTypeComputer;
import org.apache.asterix.om.typecomputer.impl.IfMissingTypeComputer;
import org.apache.asterix.om.typecomputer.impl.IfNanOrInfTypeComputer;
import org.apache.asterix.om.typecomputer.impl.IfNullTypeComputer;
import org.apache.asterix.om.typecomputer.impl.InjectFailureTypeComputer;
import org.apache.asterix.om.typecomputer.impl.Int64ArrayToStringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.LocalAvgTypeComputer;
import org.apache.asterix.om.typecomputer.impl.LocalSingleVarStatisticsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.MinMaxAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.MissingIfTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NonTaggedGetItemResultType;
import org.apache.asterix.om.typecomputer.impl.NotUnknownTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NullIfTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NullableDoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NullableTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericAddSubMulDivTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericBinaryToDoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericDivideTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericRoundTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericSumAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericUnaryTypeComputer;
import org.apache.asterix.om.typecomputer.impl.OpenARecordTypeComputer;
import org.apache.asterix.om.typecomputer.impl.OpenRecordConstructorResultType;
import org.apache.asterix.om.typecomputer.impl.OrderedListConstructorTypeComputer;
import org.apache.asterix.om.typecomputer.impl.OrderedListOfAInt32TypeComputer;
import org.apache.asterix.om.typecomputer.impl.OrderedListOfAIntervalTypeComputer;
import org.apache.asterix.om.typecomputer.impl.OrderedListOfAPointTypeComputer;
import org.apache.asterix.om.typecomputer.impl.OrderedListOfAStringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.OrderedListOfAnyTypeComputer;
import org.apache.asterix.om.typecomputer.impl.PropagateTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordAddFieldsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordAddTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordMergeTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordPutTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordRemoveFieldsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordRemoveTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordRenameTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ScalarArrayAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ScalarVersionOfAggregateResultType;
import org.apache.asterix.om.typecomputer.impl.SleepTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringJoinTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SubsetCollectionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SwitchCaseComputer;
import org.apache.asterix.om.typecomputer.impl.ToArrayTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToBigIntTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToDoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToNumberTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToObjectTypeComputer;
import org.apache.asterix.om.typecomputer.impl.TreatAsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryBinaryInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.UniformInputTypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnionMbrAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnorderedListConstructorTypeComputer;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.properties.UnpartitionedPropertyComputer;

public class BuiltinFunctions {

    private static final Map<FunctionIdentifier, BuiltinFunctionInfo> registeredFunctions = new HashMap<>();
    private static final Map<FunctionIdentifier, Set<? extends BuiltinFunctionProperty>> builtinFunctionProperties =
            new HashMap<>();

    private static final Map<FunctionIdentifier, IFunctionToDataSourceRewriter> datasourceFunctions = new HashMap<>();
    private static final Map<FunctionIdentifier, Boolean> builtinUnnestingFunctions = new HashMap<>();

    private static final Set<FunctionIdentifier> builtinAggregateFunctions = new HashSet<>();
    private static final Set<FunctionIdentifier> globalAggregateFunctions = new HashSet<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> aggregateToLocalAggregate = new HashMap<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> aggregateToIntermediateAggregate = new HashMap<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> aggregateToGlobalAggregate = new HashMap<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> aggregateToSerializableAggregate = new HashMap<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> scalarToAggregateFunctionMap = new HashMap<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> distinctToRegularAggregateFunctionMap =
            new HashMap<>();
    private static final Map<FunctionIdentifier, FunctionIdentifier> sqlToWindowFunctions = new HashMap<>();
    private static final Set<FunctionIdentifier> windowFunctions = new HashSet<>();

    private static final Map<FunctionIdentifier, SpatialFilterKind> spatialFilterFunctions = new HashMap<>();
    private static final Set<FunctionIdentifier> similarityFunctions = new HashSet<>();

    public static final FunctionIdentifier TYPE_OF = FunctionConstants.newAsterix("type-of", 1);
    public static final FunctionIdentifier GET_HANDLE = FunctionConstants.newAsterix("get-handle", 2);
    public static final FunctionIdentifier GET_DATA = FunctionConstants.newAsterix("get-data", 2);
    public static final FunctionIdentifier GET_ITEM = FunctionConstants.newAsterix("get-item", 2);
    public static final FunctionIdentifier ANY_COLLECTION_MEMBER =
            FunctionConstants.newAsterix("any-collection-member", 1);
    public static final FunctionIdentifier LEN = FunctionConstants.newAsterix("len", 1);
    public static final FunctionIdentifier CONCAT_NON_NULL =
            FunctionConstants.newAsterix("concat-non-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier EMPTY_STREAM = FunctionConstants.newAsterix("empty-stream", 0);
    public static final FunctionIdentifier NON_EMPTY_STREAM = FunctionConstants.newAsterix("non-empty-stream", 0);
    public static final FunctionIdentifier ORDERED_LIST_CONSTRUCTOR =
            FunctionConstants.newAsterix("ordered-list-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier UNORDERED_LIST_CONSTRUCTOR =
            FunctionConstants.newAsterix("unordered-list-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier GROUPING =
            FunctionConstants.newAsterix("grouping", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier DEEP_EQUAL = FunctionConstants.newAsterix("deep-equal", 2);

    // array functions
    public static final FunctionIdentifier ARRAY_REMOVE =
            FunctionConstants.newAsterix("array-remove", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_PUT =
            FunctionConstants.newAsterix("array-put", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_PREPEND =
            FunctionConstants.newAsterix("array-prepend", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_APPEND =
            FunctionConstants.newAsterix("array-append", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_INSERT =
            FunctionConstants.newAsterix("array-insert", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_POSITION = FunctionConstants.newAsterix("array-position", 2);
    public static final FunctionIdentifier ARRAY_REPEAT = FunctionConstants.newAsterix("array-repeat", 2);
    public static final FunctionIdentifier ARRAY_REVERSE = FunctionConstants.newAsterix("array-reverse", 1);
    public static final FunctionIdentifier ARRAY_CONTAINS = FunctionConstants.newAsterix("array-contains", 2);
    public static final FunctionIdentifier ARRAY_DISTINCT = FunctionConstants.newAsterix("array-distinct", 1);
    public static final FunctionIdentifier ARRAY_SORT = FunctionConstants.newAsterix("array-sort", 1);
    public static final FunctionIdentifier ARRAY_UNION =
            FunctionConstants.newAsterix("array-union", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_INTERSECT =
            FunctionConstants.newAsterix("array-intersect", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_IFNULL = FunctionConstants.newAsterix("array-ifnull", 1);
    public static final FunctionIdentifier ARRAY_CONCAT =
            FunctionConstants.newAsterix("array-concat", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_RANGE_WITHOUT_STEP = FunctionConstants.newAsterix("array-range", 2);
    public static final FunctionIdentifier ARRAY_RANGE_WITH_STEP = FunctionConstants.newAsterix("array-range", 3);
    public static final FunctionIdentifier ARRAY_FLATTEN = FunctionConstants.newAsterix("array-flatten", 2);
    public static final FunctionIdentifier ARRAY_REPLACE_WITHOUT_MAXIMUM =
            FunctionConstants.newAsterix("array-replace", 3);
    public static final FunctionIdentifier ARRAY_REPLACE_WITH_MAXIMUM =
            FunctionConstants.newAsterix("array-replace", 4);
    public static final FunctionIdentifier ARRAY_SYMDIFF =
            FunctionConstants.newAsterix("array-symdiff", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_SYMDIFFN =
            FunctionConstants.newAsterix("array-symdiffn", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_STAR = FunctionConstants.newAsterix("array-star", 1);
    public static final FunctionIdentifier ARRAY_SLICE_WITHOUT_END_POSITION =
            FunctionConstants.newAsterix("array-slice", 2);
    public static final FunctionIdentifier ARRAY_SLICE_WITH_END_POSITION =
            FunctionConstants.newAsterix("array-slice", 3);
    public static final FunctionIdentifier ARRAY_EXCEPT = FunctionConstants.newAsterix("array-except", 2);
    public static final FunctionIdentifier ARRAY_SWAP = FunctionConstants.newAsterix("array-swap", 3);
    public static final FunctionIdentifier ARRAY_MOVE = FunctionConstants.newAsterix("array-move", 3);
    public static final FunctionIdentifier ARRAY_BINARY_SEARCH = FunctionConstants.newAsterix("array-binary-search", 2);

    // objects
    public static final FunctionIdentifier RECORD_MERGE = FunctionConstants.newAsterix("object-merge", 2);
    public static final FunctionIdentifier RECORD_MERGE_IGNORE_DUPLICATES =
            FunctionConstants.newAsterix("object-merge-ignore-duplicates", 2);
    public static final FunctionIdentifier RECORD_CONCAT =
            FunctionConstants.newAsterix("object-concat", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RECORD_CONCAT_STRICT =
            FunctionConstants.newAsterix("object-concat-strict", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier REMOVE_FIELDS = FunctionConstants.newAsterix("object-remove-fields", 2);
    public static final FunctionIdentifier ADD_FIELDS = FunctionConstants.newAsterix("object-add-fields", 2);

    public static final FunctionIdentifier CLOSED_RECORD_CONSTRUCTOR =
            FunctionConstants.newAsterix("closed-object-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier OPEN_RECORD_CONSTRUCTOR =
            FunctionConstants.newAsterix("open-object-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier FIELD_ACCESS_BY_INDEX =
            FunctionConstants.newAsterix("field-access-by-index", 2);
    public static final FunctionIdentifier FIELD_ACCESS_BY_NAME =
            FunctionConstants.newAsterix("field-access-by-name", 2);
    public static final FunctionIdentifier FIELD_ACCESS_NESTED = FunctionConstants.newAsterix("field-access-nested", 2);
    public static final FunctionIdentifier GET_RECORD_FIELDS = FunctionConstants.newAsterix("get-object-fields", 1);
    public static final FunctionIdentifier GET_RECORD_FIELD_VALUE =
            FunctionConstants.newAsterix("get-object-field-value", 2);
    public static final FunctionIdentifier RECORD_LENGTH = FunctionConstants.newAsterix("object-length", 1);
    public static final FunctionIdentifier RECORD_NAMES = FunctionConstants.newAsterix("object-names", 1);
    public static final FunctionIdentifier RECORD_PAIRS =
            FunctionConstants.newAsterix("object-pairs", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier GEOMETRY_CONSTRUCTOR =
            FunctionConstants.newAsterix("st-geom-from-geojson", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RECORD_REMOVE = FunctionConstants.newAsterix("object-remove", 2);
    public static final FunctionIdentifier RECORD_RENAME = FunctionConstants.newAsterix("object-rename", 3);
    public static final FunctionIdentifier RECORD_UNWRAP = FunctionConstants.newAsterix("object-unwrap", 1);
    public static final FunctionIdentifier RECORD_REPLACE = FunctionConstants.newAsterix("object-replace", 3);
    public static final FunctionIdentifier RECORD_ADD = FunctionConstants.newAsterix("object-add", 3);
    public static final FunctionIdentifier RECORD_PUT = FunctionConstants.newAsterix("object-put", 3);
    public static final FunctionIdentifier RECORD_VALUES = FunctionConstants.newAsterix("object-values", 1);
    public static final FunctionIdentifier PAIRS = FunctionConstants.newAsterix("pairs", 1);

    // numeric
    public static final FunctionIdentifier NUMERIC_UNARY_MINUS = FunctionConstants.newAsterix("numeric-unary-minus", 1);
    public static final FunctionIdentifier NUMERIC_SUBTRACT = FunctionConstants.newAsterix("numeric-subtract", 2);
    public static final FunctionIdentifier NUMERIC_MULTIPLY = FunctionConstants.newAsterix("numeric-multiply", 2);
    public static final FunctionIdentifier NUMERIC_DIVIDE = FunctionConstants.newAsterix("numeric-divide", 2);
    public static final FunctionIdentifier NUMERIC_MOD = FunctionConstants.newAsterix("numeric-mod", 2);
    public static final FunctionIdentifier NUMERIC_DIV = FunctionConstants.newAsterix("numeric-div", 2);
    public static final FunctionIdentifier NUMERIC_POWER = FunctionConstants.newAsterix("power", 2);
    public static final FunctionIdentifier NUMERIC_ABS = FunctionConstants.newAsterix("abs", 1);
    public static final FunctionIdentifier NUMERIC_ACOS = FunctionConstants.newAsterix("acos", 1);
    public static final FunctionIdentifier NUMERIC_ASIN = FunctionConstants.newAsterix("asin", 1);
    public static final FunctionIdentifier NUMERIC_ATAN = FunctionConstants.newAsterix("atan", 1);
    public static final FunctionIdentifier NUMERIC_ATAN2 = FunctionConstants.newAsterix("atan2", 2);
    public static final FunctionIdentifier NUMERIC_DEGREES = FunctionConstants.newAsterix("degrees", 1);
    public static final FunctionIdentifier NUMERIC_RADIANS = FunctionConstants.newAsterix("radians", 1);
    public static final FunctionIdentifier NUMERIC_COS = FunctionConstants.newAsterix("cos", 1);
    public static final FunctionIdentifier NUMERIC_COSH = FunctionConstants.newAsterix("cosh", 1);
    public static final FunctionIdentifier NUMERIC_SIN = FunctionConstants.newAsterix("sin", 1);
    public static final FunctionIdentifier NUMERIC_SINH = FunctionConstants.newAsterix("sinh", 1);
    public static final FunctionIdentifier NUMERIC_TAN = FunctionConstants.newAsterix("tan", 1);
    public static final FunctionIdentifier NUMERIC_TANH = FunctionConstants.newAsterix("tanh", 1);
    public static final FunctionIdentifier NUMERIC_EXP = FunctionConstants.newAsterix("exp", 1);
    public static final FunctionIdentifier NUMERIC_LN = FunctionConstants.newAsterix("ln", 1);
    public static final FunctionIdentifier NUMERIC_LOG = FunctionConstants.newAsterix("log", 1);
    public static final FunctionIdentifier NUMERIC_SQRT = FunctionConstants.newAsterix("sqrt", 1);
    public static final FunctionIdentifier NUMERIC_SIGN = FunctionConstants.newAsterix("sign", 1);
    public static final FunctionIdentifier NUMERIC_E = FunctionConstants.newAsterix("e", 0);
    public static final FunctionIdentifier NUMERIC_PI = FunctionConstants.newAsterix("pi", 0);

    public static final FunctionIdentifier NUMERIC_CEILING = FunctionConstants.newAsterix("ceiling", 1);
    public static final FunctionIdentifier NUMERIC_FLOOR = FunctionConstants.newAsterix("floor", 1);
    public static final FunctionIdentifier NUMERIC_ROUND = FunctionConstants.newAsterix("round", 1);
    public static final FunctionIdentifier NUMERIC_ROUND_WITH_ROUND_DIGIT = FunctionConstants.newAsterix("round", 2);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN =
            FunctionConstants.newAsterix("round-half-to-even", 1);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN2 =
            FunctionConstants.newAsterix("round-half-to-even", 2);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_UP2 = FunctionConstants.newAsterix("round-half-up", 2);
    public static final FunctionIdentifier NUMERIC_TRUNC = FunctionConstants.newAsterix("trunc", 2);

    // binary functions
    public static final FunctionIdentifier BINARY_LENGTH = FunctionConstants.newAsterix("binary-length", 1);
    public static final FunctionIdentifier PARSE_BINARY = FunctionConstants.newAsterix("parse-binary", 2);
    public static final FunctionIdentifier PRINT_BINARY = FunctionConstants.newAsterix("print-binary", 2);
    public static final FunctionIdentifier BINARY_CONCAT = FunctionConstants.newAsterix("binary-concat", 1);
    public static final FunctionIdentifier SUBBINARY_FROM = FunctionConstants.newAsterix("sub-binary", 2);
    public static final FunctionIdentifier SUBBINARY_FROM_TO = FunctionConstants.newAsterix("sub-binary", 3);
    public static final FunctionIdentifier FIND_BINARY = FunctionConstants.newAsterix("find-binary", 2);
    public static final FunctionIdentifier FIND_BINARY_FROM = FunctionConstants.newAsterix("find-binary", 3);

    // bitwise functions
    public static final FunctionIdentifier BIT_AND = FunctionConstants.newAsterix("bitand", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier BIT_OR = FunctionConstants.newAsterix("bitor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier BIT_XOR = FunctionConstants.newAsterix("bitxor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier BIT_NOT = FunctionConstants.newAsterix("bitnot", 1);
    public static final FunctionIdentifier BIT_COUNT = FunctionConstants.newAsterix("bitcount", 1);
    public static final FunctionIdentifier BIT_SET = FunctionConstants.newAsterix("bitset", 2);
    public static final FunctionIdentifier BIT_CLEAR = FunctionConstants.newAsterix("bitclear", 2);
    public static final FunctionIdentifier BIT_SHIFT_WITHOUT_ROTATE_FLAG = FunctionConstants.newAsterix("bitshift", 2);
    public static final FunctionIdentifier BIT_SHIFT_WITH_ROTATE_FLAG = FunctionConstants.newAsterix("bitshift", 3);
    public static final FunctionIdentifier BIT_TEST_WITHOUT_ALL_FLAG = FunctionConstants.newAsterix("bittest", 2);
    public static final FunctionIdentifier BIT_TEST_WITH_ALL_FLAG = FunctionConstants.newAsterix("bittest", 3);
    public static final FunctionIdentifier IS_BIT_SET_WITHOUT_ALL_FLAG = FunctionConstants.newAsterix("isbitset", 2);
    public static final FunctionIdentifier IS_BIT_SET_WITH_ALL_FLAG = FunctionConstants.newAsterix("isbitset", 3);

    // String functions
    public static final FunctionIdentifier STRING_EQUAL = FunctionConstants.newAsterix("string-equal", 2);
    public static final FunctionIdentifier STRING_MATCHES = FunctionConstants.newAsterix("matches", 2);
    public static final FunctionIdentifier STRING_MATCHES_WITH_FLAG = FunctionConstants.newAsterix("matches", 3);
    public static final FunctionIdentifier STRING_REGEXP_LIKE = FunctionConstants.newAsterix("regexp-like", 2);
    public static final FunctionIdentifier STRING_REGEXP_LIKE_WITH_FLAG =
            FunctionConstants.newAsterix("regexp-like", 3);
    public static final FunctionIdentifier STRING_REGEXP_POSITION = FunctionConstants.newAsterix("regexp-position", 2);
    public static final FunctionIdentifier STRING_REGEXP_POSITION_OFFSET_1 =
            FunctionConstants.newAsterix("regexp-position1", 2);
    public static final FunctionIdentifier STRING_REGEXP_POSITION_WITH_FLAG =
            FunctionConstants.newAsterix("regexp-position", 3);
    public static final FunctionIdentifier STRING_REGEXP_POSITION_OFFSET_1_WITH_FLAG =
            FunctionConstants.newAsterix("regexp-position1", 3);
    public static final FunctionIdentifier STRING_REGEXP_REPLACE = FunctionConstants.newAsterix("regexp-replace", 3);
    public static final FunctionIdentifier STRING_REGEXP_REPLACE_WITH_FLAG =
            FunctionConstants.newAsterix("regexp-replace", 4);
    public static final FunctionIdentifier STRING_REGEXP_MATCHES = FunctionConstants.newAsterix("regexp-matches", 2);
    public static final FunctionIdentifier STRING_REGEXP_SPLIT = FunctionConstants.newAsterix("regexp-split", 2);
    public static final FunctionIdentifier STRING_LOWERCASE = FunctionConstants.newAsterix("lowercase", 1);
    public static final FunctionIdentifier STRING_UPPERCASE = FunctionConstants.newAsterix("uppercase", 1);
    public static final FunctionIdentifier STRING_INITCAP = FunctionConstants.newAsterix("initcap", 1);
    public static final FunctionIdentifier STRING_TRIM = FunctionConstants.newAsterix("trim", 1);
    public static final FunctionIdentifier STRING_LTRIM = FunctionConstants.newAsterix("ltrim", 1);
    public static final FunctionIdentifier STRING_RTRIM = FunctionConstants.newAsterix("rtrim", 1);
    public static final FunctionIdentifier STRING_TRIM2 = FunctionConstants.newAsterix("trim", 2);
    public static final FunctionIdentifier STRING_LTRIM2 = FunctionConstants.newAsterix("ltrim", 2);
    public static final FunctionIdentifier STRING_RTRIM2 = FunctionConstants.newAsterix("rtrim", 2);
    public static final FunctionIdentifier STRING_POSITION = FunctionConstants.newAsterix("position", 2);
    public static final FunctionIdentifier STRING_POSITION_OFFSET_1 = FunctionConstants.newAsterix("position1", 2);
    public static final FunctionIdentifier STRING_REPLACE = FunctionConstants.newAsterix("replace", 3);
    public static final FunctionIdentifier STRING_REPLACE_WITH_LIMIT = FunctionConstants.newAsterix("replace", 4);
    public static final FunctionIdentifier STRING_REVERSE = FunctionConstants.newAsterix("reverse", 1);
    public static final FunctionIdentifier STRING_LENGTH = FunctionConstants.newAsterix("string-length", 1);
    public static final FunctionIdentifier STRING_LIKE = FunctionConstants.newAsterix("like", 2);
    public static final FunctionIdentifier STRING_CONTAINS = FunctionConstants.newAsterix("contains", 2);
    public static final FunctionIdentifier STRING_STARTS_WITH = FunctionConstants.newAsterix("starts-with", 2);
    public static final FunctionIdentifier STRING_ENDS_WITH = FunctionConstants.newAsterix("ends-with", 2);
    public static final FunctionIdentifier SUBSTRING = FunctionConstants.newAsterix("substring", 3);
    public static final FunctionIdentifier SUBSTRING_OFFSET_1 = FunctionConstants.newAsterix("substring1", 3);
    public static final FunctionIdentifier SUBSTRING2 = FunctionConstants.newAsterix("substring", 2);
    public static final FunctionIdentifier SUBSTRING2_OFFSET_1 = FunctionConstants.newAsterix("substring1", 2);
    public static final FunctionIdentifier SUBSTRING_BEFORE = FunctionConstants.newAsterix("substring-before", 2);
    public static final FunctionIdentifier SUBSTRING_AFTER = FunctionConstants.newAsterix("substring-after", 2);
    public static final FunctionIdentifier STRING_TO_CODEPOINT = FunctionConstants.newAsterix("string-to-codepoint", 1);
    public static final FunctionIdentifier CODEPOINT_TO_STRING = FunctionConstants.newAsterix("codepoint-to-string", 1);
    public static final FunctionIdentifier STRING_CONCAT = FunctionConstants.newAsterix("string-concat", 1);
    public static final FunctionIdentifier STRING_JOIN = FunctionConstants.newAsterix("string-join", 2);
    public static final FunctionIdentifier STRING_REPEAT = FunctionConstants.newAsterix("repeat", 2);
    public static final FunctionIdentifier STRING_SPLIT = FunctionConstants.newAsterix("split", 2);
    public static final FunctionIdentifier STRING_PARSE_JSON = FunctionConstants.newAsterix("parse-json", 1);

    public static final FunctionIdentifier DATASET =
            FunctionConstants.newAsterix("dataset", FunctionIdentifier.VARARGS); // 1, 2 or 3
    public static final FunctionIdentifier FEED_COLLECT = FunctionConstants.newAsterix("feed-collect", 7);
    public static final FunctionIdentifier FEED_INTERCEPT = FunctionConstants.newAsterix("feed-intercept", 1);

    public static final FunctionIdentifier INDEX_SEARCH =
            FunctionConstants.newAsterix("index-search", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier MAKE_FIELD_INDEX_HANDLE =
            FunctionConstants.newAsterix("make-field-index-handle", 2);
    public static final FunctionIdentifier MAKE_FIELD_NESTED_HANDLE =
            FunctionConstants.newAsterix("make-field-nested-handle", 3);
    public static final FunctionIdentifier MAKE_FIELD_NAME_HANDLE =
            FunctionConstants.newAsterix("make-field-name-handle", 1);

    // aggregate functions
    public static final FunctionIdentifier LISTIFY = FunctionConstants.newAsterix("listify", 1);
    public static final FunctionIdentifier AVG = FunctionConstants.newAsterix("agg-avg", 1);
    public static final FunctionIdentifier COUNT = FunctionConstants.newAsterix("agg-count", 1);
    public static final FunctionIdentifier SUM = FunctionConstants.newAsterix("agg-sum", 1);
    public static final FunctionIdentifier LOCAL_SUM = FunctionConstants.newAsterix("agg-local-sum", 1);
    public static final FunctionIdentifier INTERMEDIATE_SUM = FunctionConstants.newAsterix("agg-intermediate-sum", 1);
    public static final FunctionIdentifier GLOBAL_SUM = FunctionConstants.newAsterix("agg-global-sum", 1);
    public static final FunctionIdentifier MAX = FunctionConstants.newAsterix("agg-max", 1);
    public static final FunctionIdentifier LOCAL_MAX = FunctionConstants.newAsterix("agg-local-max", 1);
    public static final FunctionIdentifier INTERMEDIATE_MAX = FunctionConstants.newAsterix("agg-intermediate-max", 1);
    public static final FunctionIdentifier GLOBAL_MAX = FunctionConstants.newAsterix("agg-global-max", 1);
    public static final FunctionIdentifier MIN = FunctionConstants.newAsterix("agg-min", 1);
    public static final FunctionIdentifier LOCAL_MIN = FunctionConstants.newAsterix("agg-local-min", 1);
    public static final FunctionIdentifier INTERMEDIATE_MIN = FunctionConstants.newAsterix("agg-intermediate-min", 1);
    public static final FunctionIdentifier GLOBAL_MIN = FunctionConstants.newAsterix("agg-global-min", 1);
    public static final FunctionIdentifier GLOBAL_AVG = FunctionConstants.newAsterix("agg-global-avg", 1);
    public static final FunctionIdentifier INTERMEDIATE_AVG = FunctionConstants.newAsterix("agg-intermediate-avg", 1);
    public static final FunctionIdentifier LOCAL_AVG = FunctionConstants.newAsterix("agg-local-avg", 1);
    public static final FunctionIdentifier FIRST_ELEMENT = FunctionConstants.newAsterix("agg-first-element", 1);
    public static final FunctionIdentifier LOCAL_FIRST_ELEMENT =
            FunctionConstants.newAsterix("agg-local-first-element", 1);
    public static final FunctionIdentifier LAST_ELEMENT = FunctionConstants.newAsterix("agg-last-element", 1);
    public static final FunctionIdentifier STDDEV_SAMP = FunctionConstants.newAsterix("agg-stddev_samp", 1);
    public static final FunctionIdentifier GLOBAL_STDDEV_SAMP =
            FunctionConstants.newAsterix("agg-global-stddev_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_STDDEV_SAMP =
            FunctionConstants.newAsterix("agg-intermediate-stddev_samp", 1);
    public static final FunctionIdentifier LOCAL_STDDEV_SAMP = FunctionConstants.newAsterix("agg-local-stddev_samp", 1);
    public static final FunctionIdentifier LOCAL_SAMPLING =
            FunctionConstants.newAsterix("agg-local-sampling", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RANGE_MAP =
            FunctionConstants.newAsterix("agg-range-map", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier STDDEV_POP = FunctionConstants.newAsterix("agg-stddev_pop", 1);
    public static final FunctionIdentifier GLOBAL_STDDEV_POP = FunctionConstants.newAsterix("agg-global-stddev_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_STDDEV_POP =
            FunctionConstants.newAsterix("agg-intermediate-stddev_pop", 1);
    public static final FunctionIdentifier LOCAL_STDDEV_POP = FunctionConstants.newAsterix("agg-local-stddev_pop", 1);
    public static final FunctionIdentifier VAR_SAMP = FunctionConstants.newAsterix("agg-var_samp", 1);
    public static final FunctionIdentifier GLOBAL_VAR_SAMP = FunctionConstants.newAsterix("agg-global-var_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_VAR_SAMP =
            FunctionConstants.newAsterix("agg-intermediate-var_samp", 1);
    public static final FunctionIdentifier LOCAL_VAR_SAMP = FunctionConstants.newAsterix("agg-local-var_samp", 1);
    public static final FunctionIdentifier VAR_POP = FunctionConstants.newAsterix("agg-var_pop", 1);
    public static final FunctionIdentifier GLOBAL_VAR_POP = FunctionConstants.newAsterix("agg-global-var_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_VAR_POP =
            FunctionConstants.newAsterix("agg-intermediate-var_pop", 1);
    public static final FunctionIdentifier LOCAL_VAR_POP = FunctionConstants.newAsterix("agg-local-var_pop", 1);
    public static final FunctionIdentifier SKEWNESS = FunctionConstants.newAsterix("agg-skewness", 1);
    public static final FunctionIdentifier GLOBAL_SKEWNESS = FunctionConstants.newAsterix("agg-global-skewness", 1);
    public static final FunctionIdentifier INTERMEDIATE_SKEWNESS =
            FunctionConstants.newAsterix("agg-intermediate-skewness", 1);
    public static final FunctionIdentifier LOCAL_SKEWNESS = FunctionConstants.newAsterix("agg-local-skewness", 1);
    public static final FunctionIdentifier KURTOSIS = FunctionConstants.newAsterix("agg-kurtosis", 1);
    public static final FunctionIdentifier GLOBAL_KURTOSIS = FunctionConstants.newAsterix("agg-global-kurtosis", 1);
    public static final FunctionIdentifier INTERMEDIATE_KURTOSIS =
            FunctionConstants.newAsterix("agg-intermediate-kurtosis", 1);
    public static final FunctionIdentifier LOCAL_KURTOSIS = FunctionConstants.newAsterix("agg-local-kurtosis", 1);
    public static final FunctionIdentifier NULL_WRITER = FunctionConstants.newAsterix("agg-null-writer", 1);
    public static final FunctionIdentifier UNION_MBR = FunctionConstants.newAsterix("agg-union_mbr", 1);
    public static final FunctionIdentifier LOCAL_UNION_MBR = FunctionConstants.newAsterix("agg-local-union_mbr", 1);
    public static final FunctionIdentifier INTERMEDIATE_UNION_MBR =
            FunctionConstants.newAsterix("agg-intermediate-union_mbr", 1);
    public static final FunctionIdentifier GLOBAL_UNION_MBR = FunctionConstants.newAsterix("agg-global-union_mbr", 1);

    public static final FunctionIdentifier SCALAR_ARRAYAGG = FunctionConstants.newAsterix("arrayagg", 1);
    public static final FunctionIdentifier SCALAR_AVG = FunctionConstants.newAsterix("avg", 1);
    public static final FunctionIdentifier SCALAR_COUNT = FunctionConstants.newAsterix("count", 1);
    public static final FunctionIdentifier SCALAR_SUM = FunctionConstants.newAsterix("sum", 1);
    public static final FunctionIdentifier SCALAR_MAX = FunctionConstants.newAsterix("max", 1);
    public static final FunctionIdentifier SCALAR_MIN = FunctionConstants.newAsterix("min", 1);
    public static final FunctionIdentifier SCALAR_FIRST_ELEMENT = FunctionConstants.newAsterix("first-element", 1);
    public static final FunctionIdentifier SCALAR_LOCAL_FIRST_ELEMENT =
            FunctionConstants.newAsterix("local-first-element", 1);
    public static final FunctionIdentifier SCALAR_LAST_ELEMENT = FunctionConstants.newAsterix("last-element", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_SAMP = FunctionConstants.newAsterix("stddev_samp", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_POP = FunctionConstants.newAsterix("stddev_pop", 1);
    public static final FunctionIdentifier SCALAR_VAR_SAMP = FunctionConstants.newAsterix("var_samp", 1);
    public static final FunctionIdentifier SCALAR_VAR_POP = FunctionConstants.newAsterix("var_pop", 1);
    public static final FunctionIdentifier SCALAR_SKEWNESS = FunctionConstants.newAsterix("skewness", 1);
    public static final FunctionIdentifier SCALAR_KURTOSIS = FunctionConstants.newAsterix("kurtosis", 1);
    public static final FunctionIdentifier SCALAR_UNION_MBR = FunctionConstants.newAsterix("union_mbr", 1);

    // serializable aggregate functions
    public static final FunctionIdentifier SERIAL_AVG = FunctionConstants.newAsterix("avg-serial", 1);
    public static final FunctionIdentifier SERIAL_COUNT = FunctionConstants.newAsterix("count-serial", 1);
    public static final FunctionIdentifier SERIAL_SUM = FunctionConstants.newAsterix("sum-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SUM = FunctionConstants.newAsterix("local-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SUM =
            FunctionConstants.newAsterix("intermediate-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SUM = FunctionConstants.newAsterix("global-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_AVG = FunctionConstants.newAsterix("global-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_AVG = FunctionConstants.newAsterix("local-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_AVG =
            FunctionConstants.newAsterix("intermediate-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_STDDEV_SAMP = FunctionConstants.newAsterix("stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_STDDEV_SAMP =
            FunctionConstants.newAsterix("global-stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_STDDEV_SAMP =
            FunctionConstants.newAsterix("local-stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_STDDEV_SAMP =
            FunctionConstants.newAsterix("intermediate-stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_STDDEV_POP = FunctionConstants.newAsterix("stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_STDDEV_POP =
            FunctionConstants.newAsterix("global-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_STDDEV_POP =
            FunctionConstants.newAsterix("local-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_STDDEV_POP =
            FunctionConstants.newAsterix("intermediate-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_VAR_SAMP = FunctionConstants.newAsterix("var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_VAR_SAMP =
            FunctionConstants.newAsterix("global-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_VAR_SAMP =
            FunctionConstants.newAsterix("local-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_VAR_SAMP =
            FunctionConstants.newAsterix("intermediate-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_VAR_POP = FunctionConstants.newAsterix("var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_VAR_POP =
            FunctionConstants.newAsterix("global-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_VAR_POP =
            FunctionConstants.newAsterix("local-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_VAR_POP =
            FunctionConstants.newAsterix("intermediate-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_SKEWNESS = FunctionConstants.newAsterix("skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SKEWNESS =
            FunctionConstants.newAsterix("global-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SKEWNESS =
            FunctionConstants.newAsterix("local-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SKEWNESS =
            FunctionConstants.newAsterix("intermediate-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_KURTOSIS = FunctionConstants.newAsterix("kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_KURTOSIS =
            FunctionConstants.newAsterix("global-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_KURTOSIS =
            FunctionConstants.newAsterix("local-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_KURTOSIS =
            FunctionConstants.newAsterix("intermediate-kurtosis-serial", 1);

    // distinct aggregate functions
    public static final FunctionIdentifier LISTIFY_DISTINCT = FunctionConstants.newAsterix("listify-distinct", 1);
    public static final FunctionIdentifier SCALAR_ARRAYAGG_DISTINCT =
            FunctionConstants.newAsterix("arrayagg-distinct", 1);
    public static final FunctionIdentifier COUNT_DISTINCT = FunctionConstants.newAsterix("agg-count-distinct", 1);
    public static final FunctionIdentifier SCALAR_COUNT_DISTINCT = FunctionConstants.newAsterix("count-distinct", 1);
    public static final FunctionIdentifier SUM_DISTINCT = FunctionConstants.newAsterix("agg-sum-distinct", 1);
    public static final FunctionIdentifier SCALAR_SUM_DISTINCT = FunctionConstants.newAsterix("sum-distinct", 1);
    public static final FunctionIdentifier AVG_DISTINCT = FunctionConstants.newAsterix("agg-avg-distinct", 1);
    public static final FunctionIdentifier SCALAR_AVG_DISTINCT = FunctionConstants.newAsterix("avg-distinct", 1);
    public static final FunctionIdentifier MAX_DISTINCT = FunctionConstants.newAsterix("agg-max-distinct", 1);
    public static final FunctionIdentifier SCALAR_MAX_DISTINCT = FunctionConstants.newAsterix("max-distinct", 1);
    public static final FunctionIdentifier MIN_DISTINCT = FunctionConstants.newAsterix("agg-min-distinct", 1);
    public static final FunctionIdentifier SCALAR_MIN_DISTINCT = FunctionConstants.newAsterix("min-distinct", 1);
    public static final FunctionIdentifier STDDEV_SAMP_DISTINCT =
            FunctionConstants.newAsterix("agg-stddev_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_SAMP_DISTINCT =
            FunctionConstants.newAsterix("stddev_samp-distinct", 1);
    public static final FunctionIdentifier STDDEV_POP_DISTINCT =
            FunctionConstants.newAsterix("agg-stddev_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_POP_DISTINCT =
            FunctionConstants.newAsterix("stddev_pop-distinct", 1);
    public static final FunctionIdentifier VAR_SAMP_DISTINCT = FunctionConstants.newAsterix("agg-var_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_VAR_SAMP_DISTINCT =
            FunctionConstants.newAsterix("var_samp-distinct", 1);
    public static final FunctionIdentifier VAR_POP_DISTINCT = FunctionConstants.newAsterix("agg-var_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_VAR_POP_DISTINCT =
            FunctionConstants.newAsterix("var_pop-distinct", 1);
    public static final FunctionIdentifier SKEWNESS_DISTINCT = FunctionConstants.newAsterix("agg-skewness-distinct", 1);
    public static final FunctionIdentifier SCALAR_SKEWNESS_DISTINCT =
            FunctionConstants.newAsterix("skewness-distinct", 1);
    public static final FunctionIdentifier KURTOSIS_DISTINCT = FunctionConstants.newAsterix("agg-kurtosis-distinct", 1);
    public static final FunctionIdentifier SCALAR_KURTOSIS_DISTINCT =
            FunctionConstants.newAsterix("kurtosis-distinct", 1);

    // sql aggregate functions
    public static final FunctionIdentifier SQL_AVG = FunctionConstants.newAsterix("agg-sql-avg", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_AVG =
            FunctionConstants.newAsterix("intermediate-agg-sql-avg", 1);
    public static final FunctionIdentifier SQL_COUNT = FunctionConstants.newAsterix("agg-sql-count", 1);
    public static final FunctionIdentifier SQL_SUM = FunctionConstants.newAsterix("agg-sql-sum", 1);
    public static final FunctionIdentifier LOCAL_SQL_SUM = FunctionConstants.newAsterix("agg-local-sql-sum", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_SUM =
            FunctionConstants.newAsterix("agg-intermediate-sql-sum", 1);
    public static final FunctionIdentifier GLOBAL_SQL_SUM = FunctionConstants.newAsterix("agg-global-sql-sum", 1);
    public static final FunctionIdentifier SQL_MAX = FunctionConstants.newAsterix("agg-sql-max", 1);
    public static final FunctionIdentifier LOCAL_SQL_MAX = FunctionConstants.newAsterix("agg-local-sql-max", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_MAX =
            FunctionConstants.newAsterix("agg-intermediate-sql-max", 1);
    public static final FunctionIdentifier GLOBAL_SQL_MAX = FunctionConstants.newAsterix("agg-global-sql-max", 1);
    public static final FunctionIdentifier SQL_MIN = FunctionConstants.newAsterix("agg-sql-min", 1);
    public static final FunctionIdentifier LOCAL_SQL_MIN = FunctionConstants.newAsterix("agg-local-sql-min", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_MIN =
            FunctionConstants.newAsterix("agg-intermediate-sql-min", 1);
    public static final FunctionIdentifier GLOBAL_SQL_MIN = FunctionConstants.newAsterix("agg-global-sql-min", 1);
    public static final FunctionIdentifier GLOBAL_SQL_AVG = FunctionConstants.newAsterix("agg-global-sql-avg", 1);
    public static final FunctionIdentifier LOCAL_SQL_AVG = FunctionConstants.newAsterix("agg-local-sql-avg", 1);
    public static final FunctionIdentifier SQL_STDDEV_SAMP = FunctionConstants.newAsterix("agg-sql-stddev_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("intermediate-agg-sql-stddev_samp", 1);
    public static final FunctionIdentifier GLOBAL_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("agg-global-sql-stddev_samp", 1);
    public static final FunctionIdentifier LOCAL_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("agg-local-sql-stddev_samp", 1);
    public static final FunctionIdentifier SQL_STDDEV_POP = FunctionConstants.newAsterix("agg-sql-stddev_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("intermediate-agg-sql-stddev_pop", 1);
    public static final FunctionIdentifier GLOBAL_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("agg-global-sql-stddev_pop", 1);
    public static final FunctionIdentifier LOCAL_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("agg-local-sql-stddev_pop", 1);
    public static final FunctionIdentifier SQL_VAR_SAMP = FunctionConstants.newAsterix("agg-sql-var_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_VAR_SAMP =
            FunctionConstants.newAsterix("intermediate-agg-sql-var_samp", 1);
    public static final FunctionIdentifier GLOBAL_SQL_VAR_SAMP =
            FunctionConstants.newAsterix("agg-global-sql-var_samp", 1);
    public static final FunctionIdentifier LOCAL_SQL_VAR_SAMP =
            FunctionConstants.newAsterix("agg-local-sql-var_samp", 1);
    public static final FunctionIdentifier SQL_VAR_POP = FunctionConstants.newAsterix("agg-sql-var_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_VAR_POP =
            FunctionConstants.newAsterix("intermediate-agg-sql-var_pop", 1);
    public static final FunctionIdentifier GLOBAL_SQL_VAR_POP =
            FunctionConstants.newAsterix("agg-global-sql-var_pop", 1);
    public static final FunctionIdentifier LOCAL_SQL_VAR_POP = FunctionConstants.newAsterix("agg-local-sql-var_pop", 1);
    public static final FunctionIdentifier SQL_SKEWNESS = FunctionConstants.newAsterix("agg-sql-skewness", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_SKEWNESS =
            FunctionConstants.newAsterix("intermediate-agg-sql-skewness", 1);
    public static final FunctionIdentifier GLOBAL_SQL_SKEWNESS =
            FunctionConstants.newAsterix("agg-global-sql-skewness", 1);
    public static final FunctionIdentifier LOCAL_SQL_SKEWNESS =
            FunctionConstants.newAsterix("agg-local-sql-skewness", 1);
    public static final FunctionIdentifier SQL_KURTOSIS = FunctionConstants.newAsterix("agg-sql-kurtosis", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_KURTOSIS =
            FunctionConstants.newAsterix("intermediate-agg-sql-kurtosis", 1);
    public static final FunctionIdentifier GLOBAL_SQL_KURTOSIS =
            FunctionConstants.newAsterix("agg-global-sql-kurtosis", 1);
    public static final FunctionIdentifier LOCAL_SQL_KURTOSIS =
            FunctionConstants.newAsterix("agg-local-sql-kurtosis", 1);
    public static final FunctionIdentifier SQL_UNION_MBR = FunctionConstants.newAsterix("agg-sql-union_mbr", 1);
    public static final FunctionIdentifier LOCAL_SQL_UNION_MBR =
            FunctionConstants.newAsterix("agg-local-sql-union_mbr", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_UNION_MBR =
            FunctionConstants.newAsterix("agg-intermediate-sql-union_mbr", 1);
    public static final FunctionIdentifier GLOBAL_SQL_UNION_MBR =
            FunctionConstants.newAsterix("agg-global-sql-union_mbr", 1);

    public static final FunctionIdentifier SCALAR_SQL_AVG = FunctionConstants.newAsterix("sql-avg", 1);
    public static final FunctionIdentifier SCALAR_SQL_COUNT = FunctionConstants.newAsterix("sql-count", 1);
    public static final FunctionIdentifier SCALAR_SQL_SUM = FunctionConstants.newAsterix("sql-sum", 1);
    public static final FunctionIdentifier SCALAR_SQL_MAX = FunctionConstants.newAsterix("sql-max", 1);
    public static final FunctionIdentifier SCALAR_SQL_MIN = FunctionConstants.newAsterix("sql-min", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_SAMP = FunctionConstants.newAsterix("sql-stddev_samp", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_POP = FunctionConstants.newAsterix("sql-stddev_pop", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_SAMP = FunctionConstants.newAsterix("sql-var_samp", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_POP = FunctionConstants.newAsterix("sql-var_pop", 1);
    public static final FunctionIdentifier SCALAR_SQL_SKEWNESS = FunctionConstants.newAsterix("sql-skewness", 1);
    public static final FunctionIdentifier SCALAR_SQL_KURTOSIS = FunctionConstants.newAsterix("sql-kurtosis", 1);
    public static final FunctionIdentifier SCALAR_SQL_UNION_MBR = FunctionConstants.newAsterix("sql-union_mbr", 1);

    // serializable sql aggregate functions
    public static final FunctionIdentifier SERIAL_SQL_AVG = FunctionConstants.newAsterix("sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_COUNT = FunctionConstants.newAsterix("sql-count-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_SUM = FunctionConstants.newAsterix("sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_SUM =
            FunctionConstants.newAsterix("local-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_SUM =
            FunctionConstants.newAsterix("intermediate-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_SUM =
            FunctionConstants.newAsterix("global-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_AVG =
            FunctionConstants.newAsterix("global-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_AVG =
            FunctionConstants.newAsterix("intermediate-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_AVG =
            FunctionConstants.newAsterix("local-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("global-sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("intermediate-sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_STDDEV_SAMP =
            FunctionConstants.newAsterix("local-sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("global-sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("intermediate-sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_STDDEV_POP =
            FunctionConstants.newAsterix("local-sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_VAR_SAMP = FunctionConstants.newAsterix("sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_VAR_SAMP =
            FunctionConstants.newAsterix("global-sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_VAR_SAMP =
            FunctionConstants.newAsterix("intermediate-sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_VAR_SAMP =
            FunctionConstants.newAsterix("local-sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_VAR_POP = FunctionConstants.newAsterix("sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_VAR_POP =
            FunctionConstants.newAsterix("global-sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_VAR_POP =
            FunctionConstants.newAsterix("intermediate-sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_VAR_POP =
            FunctionConstants.newAsterix("local-sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_SKEWNESS = FunctionConstants.newAsterix("sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_SKEWNESS =
            FunctionConstants.newAsterix("global-sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_SKEWNESS =
            FunctionConstants.newAsterix("intermediate-sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_SKEWNESS =
            FunctionConstants.newAsterix("local-sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_KURTOSIS = FunctionConstants.newAsterix("sql-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_KURTOSIS =
            FunctionConstants.newAsterix("global-sql-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_KURTOSIS =
            FunctionConstants.newAsterix("intermediate-sql-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_KURTOSIS =
            FunctionConstants.newAsterix("local-sql-kurtosis-serial", 1);

    // distinct sql aggregate functions
    public static final FunctionIdentifier SQL_COUNT_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-count-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_COUNT_DISTINCT =
            FunctionConstants.newAsterix("sql-count-distinct", 1);
    public static final FunctionIdentifier SQL_SUM_DISTINCT = FunctionConstants.newAsterix("agg-sql-sum-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_SUM_DISTINCT =
            FunctionConstants.newAsterix("sql-sum-distinct", 1);
    public static final FunctionIdentifier SQL_AVG_DISTINCT = FunctionConstants.newAsterix("agg-sql-avg-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_AVG_DISTINCT =
            FunctionConstants.newAsterix("sql-avg-distinct", 1);
    public static final FunctionIdentifier SQL_MAX_DISTINCT = FunctionConstants.newAsterix("agg-sql-max-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_MAX_DISTINCT =
            FunctionConstants.newAsterix("sql-max-distinct", 1);
    public static final FunctionIdentifier SQL_MIN_DISTINCT = FunctionConstants.newAsterix("agg-sql-min-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_MIN_DISTINCT =
            FunctionConstants.newAsterix("sql-min-distinct", 1);
    public static final FunctionIdentifier SQL_STDDEV_SAMP_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-stddev_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_SAMP_DISTINCT =
            FunctionConstants.newAsterix("sql-stddev_samp-distinct", 1);
    public static final FunctionIdentifier SQL_STDDEV_POP_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-stddev_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_POP_DISTINCT =
            FunctionConstants.newAsterix("sql-stddev_pop-distinct", 1);
    public static final FunctionIdentifier SQL_VAR_SAMP_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-var_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_SAMP_DISTINCT =
            FunctionConstants.newAsterix("sql-var_samp-distinct", 1);
    public static final FunctionIdentifier SQL_VAR_POP_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-var_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_POP_DISTINCT =
            FunctionConstants.newAsterix("sql-var_pop-distinct", 1);
    public static final FunctionIdentifier SQL_SKEWNESS_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-skewness-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_SKEWNESS_DISTINCT =
            FunctionConstants.newAsterix("sql-skewness-distinct", 1);
    public static final FunctionIdentifier SQL_KURTOSIS_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-kurtosis-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_KURTOSIS_DISTINCT =
            FunctionConstants.newAsterix("sql-kurtosis-distinct", 1);

    // window functions
    public static final FunctionIdentifier CUME_DIST = FunctionConstants.newAsterix("cume_dist", 0);
    public static final FunctionIdentifier CUME_DIST_IMPL = FunctionConstants.newAsterix("cume-dist-impl", 0);
    public static final FunctionIdentifier DENSE_RANK = FunctionConstants.newAsterix("dense_rank", 0);
    public static final FunctionIdentifier DENSE_RANK_IMPL =
            FunctionConstants.newAsterix("dense-rank-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier FIRST_VALUE = FunctionConstants.newAsterix("first_value", 1);
    public static final FunctionIdentifier FIRST_VALUE_IMPL = FunctionConstants.newAsterix("first-value-impl", 2);
    public static final FunctionIdentifier LAG = FunctionConstants.newAsterix("lag", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier LAG_IMPL =
            FunctionConstants.newAsterix("lag-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier LAST_VALUE = FunctionConstants.newAsterix("last_value", 1);
    public static final FunctionIdentifier LAST_VALUE_IMPL = FunctionConstants.newAsterix("last-value-impl", 2);
    public static final FunctionIdentifier LEAD = FunctionConstants.newAsterix("lead", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier LEAD_IMPL =
            FunctionConstants.newAsterix("lead-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier NTH_VALUE = FunctionConstants.newAsterix("nth_value", 2);
    public static final FunctionIdentifier NTH_VALUE_IMPL = FunctionConstants.newAsterix("nth-value-impl", 3);
    public static final FunctionIdentifier NTILE = FunctionConstants.newAsterix("ntile", 1);
    public static final FunctionIdentifier NTILE_IMPL =
            FunctionConstants.newAsterix("ntile-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RANK = FunctionConstants.newAsterix("rank", 0);
    public static final FunctionIdentifier RANK_IMPL =
            FunctionConstants.newAsterix("rank-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RATIO_TO_REPORT = FunctionConstants.newAsterix("ratio_to_report", 1);
    public static final FunctionIdentifier RATIO_TO_REPORT_IMPL =
            FunctionConstants.newAsterix("ratio-to-report-impl", 2);
    public static final FunctionIdentifier ROW_NUMBER = FunctionConstants.newAsterix("row_number", 0);
    public static final FunctionIdentifier ROW_NUMBER_IMPL = FunctionConstants.newAsterix("row-number-impl", 0);
    public static final FunctionIdentifier PERCENT_RANK = FunctionConstants.newAsterix("percent_rank", 0);
    public static final FunctionIdentifier PERCENT_RANK_IMPL =
            FunctionConstants.newAsterix("percent-rank-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier WIN_MARK_FIRST_MISSING_IMPL =
            FunctionConstants.newAsterix("win-mark-first-missing-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier WIN_MARK_FIRST_NULL_IMPL =
            FunctionConstants.newAsterix("win-mark-first-null-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier WIN_PARTITION_LENGTH_IMPL =
            FunctionConstants.newAsterix("win-partition-length-impl", 0);

    // unnesting functions
    public static final FunctionIdentifier SCAN_COLLECTION = FunctionConstants.newAsterix("scan-collection", 1);
    public static final FunctionIdentifier SUBSET_COLLECTION = FunctionConstants.newAsterix("subset-collection", 3);

    public static final FunctionIdentifier RANGE = FunctionConstants.newAsterix("range", 2);
    public static final FunctionIdentifier SPATIAL_TILE = FunctionConstants.newAsterix("spatial-tile", 4);

    // fuzzy functions
    public static final FunctionIdentifier FUZZY_EQ = FunctionConstants.newAsterix("fuzzy-eq", 2);

    public static final FunctionIdentifier PREFIX_LEN_JACCARD = FunctionConstants.newAsterix("prefix-len-jaccard", 2);

    public static final FunctionIdentifier SIMILARITY_JACCARD = FunctionConstants.newAsterix("similarity-jaccard", 2);
    public static final FunctionIdentifier SIMILARITY_JACCARD_CHECK =
            FunctionConstants.newAsterix("similarity-jaccard-check", 3);
    public static final FunctionIdentifier SIMILARITY_JACCARD_SORTED =
            FunctionConstants.newAsterix("similarity-jaccard-sorted", 2);
    public static final FunctionIdentifier SIMILARITY_JACCARD_SORTED_CHECK =
            FunctionConstants.newAsterix("similarity-jaccard-sorted-check", 3);
    public static final FunctionIdentifier SIMILARITY_JACCARD_PREFIX =
            FunctionConstants.newAsterix("similarity-jaccard-prefix", 6);
    public static final FunctionIdentifier SIMILARITY_JACCARD_PREFIX_CHECK =
            FunctionConstants.newAsterix("similarity-jaccard-prefix-check", 6);

    public static final FunctionIdentifier EDIT_DISTANCE = FunctionConstants.newAsterix("edit-distance", 2);
    public static final FunctionIdentifier EDIT_DISTANCE_CHECK = FunctionConstants.newAsterix("edit-distance-check", 3);
    public static final FunctionIdentifier EDIT_DISTANCE_LIST_IS_FILTERABLE =
            FunctionConstants.newAsterix("edit-distance-list-is-filterable", 2);
    public static final FunctionIdentifier EDIT_DISTANCE_STRING_IS_FILTERABLE =
            FunctionConstants.newAsterix("edit-distance-string-is-filterable", 4);
    public static final FunctionIdentifier EDIT_DISTANCE_CONTAINS =
            FunctionConstants.newAsterix("edit-distance-contains", 3);

    // full-text
    public static final FunctionIdentifier FULLTEXT_CONTAINS = FunctionConstants.newAsterix("ftcontains", 3);
    // full-text without any option provided
    public static final FunctionIdentifier FULLTEXT_CONTAINS_WO_OPTION = FunctionConstants.newAsterix("ftcontains", 2);

    // tokenizers:
    public static final FunctionIdentifier WORD_TOKENS = FunctionConstants.newAsterix("word-tokens", 1);
    public static final FunctionIdentifier HASHED_WORD_TOKENS = FunctionConstants.newAsterix("hashed-word-tokens", 1);
    public static final FunctionIdentifier COUNTHASHED_WORD_TOKENS =
            FunctionConstants.newAsterix("counthashed-word-tokens", 1);
    public static final FunctionIdentifier GRAM_TOKENS = FunctionConstants.newAsterix("gram-tokens", 3);
    public static final FunctionIdentifier HASHED_GRAM_TOKENS = FunctionConstants.newAsterix("hashed-gram-tokens", 3);
    public static final FunctionIdentifier COUNTHASHED_GRAM_TOKENS =
            FunctionConstants.newAsterix("counthashed-gram-tokens", 3);

    public static final FunctionIdentifier TID = FunctionConstants.newAsterix("tid", 0);
    public static final FunctionIdentifier GTID = FunctionConstants.newAsterix("gtid", 0);

    // constructors:
    public static final FunctionIdentifier BOOLEAN_CONSTRUCTOR = FunctionConstants.newAsterix("boolean", 1);
    public static final FunctionIdentifier BOOLEAN_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("boolean-default-null", 1);
    public static final FunctionIdentifier STRING_CONSTRUCTOR = FunctionConstants.newAsterix("string", 1);
    public static final FunctionIdentifier STRING_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("string-default-null", 1);
    public static final FunctionIdentifier BINARY_HEX_CONSTRUCTOR = FunctionConstants.newAsterix("hex", 1);
    public static final FunctionIdentifier BINARY_BASE64_CONSTRUCTOR = FunctionConstants.newAsterix("base64", 1);
    public static final FunctionIdentifier BINARY_BASE64_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("base64-default-null", 1);
    public static final FunctionIdentifier INT8_CONSTRUCTOR = FunctionConstants.newAsterix("int8", 1);
    public static final FunctionIdentifier INT8_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("int8-default-null", 1);
    public static final FunctionIdentifier INT16_CONSTRUCTOR = FunctionConstants.newAsterix("int16", 1);
    public static final FunctionIdentifier INT16_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("int16-default-null", 1);
    public static final FunctionIdentifier INT32_CONSTRUCTOR = FunctionConstants.newAsterix("int32", 1);
    public static final FunctionIdentifier INT32_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("int32-default-null", 1);
    public static final FunctionIdentifier INT64_CONSTRUCTOR = FunctionConstants.newAsterix("int64", 1);
    public static final FunctionIdentifier INT64_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("int64-default-null", 1);
    public static final FunctionIdentifier FLOAT_CONSTRUCTOR = FunctionConstants.newAsterix("float", 1);
    public static final FunctionIdentifier FLOAT_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("float-default-null", 1);
    public static final FunctionIdentifier DOUBLE_CONSTRUCTOR = FunctionConstants.newAsterix("double", 1);
    public static final FunctionIdentifier DOUBLE_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("double-default-null", 1);
    public static final FunctionIdentifier POINT_CONSTRUCTOR = FunctionConstants.newAsterix("point", 1);
    public static final FunctionIdentifier POINT3D_CONSTRUCTOR = FunctionConstants.newAsterix("point3d", 1);
    public static final FunctionIdentifier LINE_CONSTRUCTOR = FunctionConstants.newAsterix("line", 1);
    public static final FunctionIdentifier CIRCLE_CONSTRUCTOR = FunctionConstants.newAsterix("circle", 1);
    public static final FunctionIdentifier RECTANGLE_CONSTRUCTOR = FunctionConstants.newAsterix("rectangle", 1);
    public static final FunctionIdentifier POLYGON_CONSTRUCTOR = FunctionConstants.newAsterix("polygon", 1);
    public static final FunctionIdentifier TIME_CONSTRUCTOR = FunctionConstants.newAsterix("time", 1);
    public static final FunctionIdentifier TIME_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("time-default-null", 1);
    public static final FunctionIdentifier TIME_CONSTRUCTOR_WITH_FORMAT = FunctionConstants.newAsterix("time", 2);
    public static final FunctionIdentifier TIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT =
            FunctionConstants.newAsterix("time-default-null", 2);
    public static final FunctionIdentifier DATE_CONSTRUCTOR = FunctionConstants.newAsterix("date", 1);
    public static final FunctionIdentifier DATE_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("date-default-null", 1);
    public static final FunctionIdentifier DATE_CONSTRUCTOR_WITH_FORMAT = FunctionConstants.newAsterix("date", 2);
    public static final FunctionIdentifier DATE_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT =
            FunctionConstants.newAsterix("date-default-null", 2);
    public static final FunctionIdentifier DATETIME_CONSTRUCTOR = FunctionConstants.newAsterix("datetime", 1);
    public static final FunctionIdentifier DATETIME_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("datetime-default-null", 1);
    public static final FunctionIdentifier DATETIME_CONSTRUCTOR_WITH_FORMAT =
            FunctionConstants.newAsterix("datetime", 2);
    public static final FunctionIdentifier DATETIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT =
            FunctionConstants.newAsterix("datetime-default-null", 2);
    public static final FunctionIdentifier DURATION_CONSTRUCTOR = FunctionConstants.newAsterix("duration", 1);
    public static final FunctionIdentifier DURATION_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("duration-default-null", 1);
    public static final FunctionIdentifier UUID_CONSTRUCTOR = FunctionConstants.newAsterix("uuid", 1);
    public static final FunctionIdentifier UUID_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("uuid-default-null", 1);

    public static final FunctionIdentifier YEAR_MONTH_DURATION_CONSTRUCTOR =
            FunctionConstants.newAsterix("year-month-duration", 1);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("year-month-duration-default-null", 1);
    public static final FunctionIdentifier DAY_TIME_DURATION_CONSTRUCTOR =
            FunctionConstants.newAsterix("day-time-duration", 1);
    public static final FunctionIdentifier DAY_TIME_DURATION_DEFAULT_NULL_CONSTRUCTOR =
            FunctionConstants.newAsterix("day-time-duration-default-null", 1);

    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR = FunctionConstants.newAsterix("interval", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATE =
            FunctionConstants.newAsterix("interval-start-from-date", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_TIME =
            FunctionConstants.newAsterix("interval-start-from-time", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATETIME =
            FunctionConstants.newAsterix("interval-start-from-datetime", 2);
    public static final FunctionIdentifier INTERVAL_BEFORE = FunctionConstants.newAsterix("interval-before", 2);
    public static final FunctionIdentifier INTERVAL_AFTER = FunctionConstants.newAsterix("interval-after", 2);
    public static final FunctionIdentifier INTERVAL_MEETS = FunctionConstants.newAsterix("interval-meets", 2);
    public static final FunctionIdentifier INTERVAL_MET_BY = FunctionConstants.newAsterix("interval-met-by", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPS = FunctionConstants.newAsterix("interval-overlaps", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPPED_BY =
            FunctionConstants.newAsterix("interval-overlapped-by", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPPING =
            FunctionConstants.newAsterix("interval-overlapping", 2);
    public static final FunctionIdentifier INTERVAL_STARTS = FunctionConstants.newAsterix("interval-starts", 2);
    public static final FunctionIdentifier INTERVAL_STARTED_BY = FunctionConstants.newAsterix("interval-started-by", 2);
    public static final FunctionIdentifier INTERVAL_COVERS = FunctionConstants.newAsterix("interval-covers", 2);
    public static final FunctionIdentifier INTERVAL_COVERED_BY = FunctionConstants.newAsterix("interval-covered-by", 2);
    public static final FunctionIdentifier INTERVAL_ENDS = FunctionConstants.newAsterix("interval-ends", 2);
    public static final FunctionIdentifier INTERVAL_ENDED_BY = FunctionConstants.newAsterix("interval-ended-by", 2);
    public static final FunctionIdentifier CURRENT_TIME = FunctionConstants.newAsterix("current-time", 0);
    public static final FunctionIdentifier CURRENT_TIME_IMMEDIATE =
            FunctionConstants.newAsterix("current-time-immediate", 0);
    public static final FunctionIdentifier CURRENT_DATE = FunctionConstants.newAsterix("current-date", 0);
    public static final FunctionIdentifier CURRENT_DATE_IMMEDIATE =
            FunctionConstants.newAsterix("current-date-immediate", 0);
    public static final FunctionIdentifier CURRENT_DATETIME = FunctionConstants.newAsterix("current-datetime", 0);
    public static final FunctionIdentifier CURRENT_DATETIME_IMMEDIATE =
            FunctionConstants.newAsterix("current-datetime-immediate", 0);
    public static final FunctionIdentifier DURATION_EQUAL = FunctionConstants.newAsterix("duration-equal", 2);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_GREATER_THAN =
            FunctionConstants.newAsterix("year-month-duration-greater-than", 2);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_LESS_THAN =
            FunctionConstants.newAsterix("year-month-duration-less-than", 2);
    public static final FunctionIdentifier DAY_TIME_DURATION_GREATER_THAN =
            FunctionConstants.newAsterix("day-time-duration-greater-than", 2);
    public static final FunctionIdentifier DAY_TIME_DURATION_LESS_THAN =
            FunctionConstants.newAsterix("day-time-duration-less-than", 2);
    public static final FunctionIdentifier DURATION_FROM_MONTHS =
            FunctionConstants.newAsterix("duration-from-months", 1);
    public static final FunctionIdentifier MONTHS_FROM_YEAR_MONTH_DURATION =
            FunctionConstants.newAsterix("months-from-year-month-duration", 1);
    public static final FunctionIdentifier DURATION_FROM_MILLISECONDS =
            FunctionConstants.newAsterix("duration-from-ms", 1);
    public static final FunctionIdentifier MILLISECONDS_FROM_DAY_TIME_DURATION =
            FunctionConstants.newAsterix("ms-from-day-time-duration", 1);

    public static final FunctionIdentifier GET_YEAR_MONTH_DURATION =
            FunctionConstants.newAsterix("get-year-month-duration", 1);
    public static final FunctionIdentifier GET_DAY_TIME_DURATION =
            FunctionConstants.newAsterix("get-day-time-duration", 1);
    public static final FunctionIdentifier DURATION_FROM_INTERVAL =
            FunctionConstants.newAsterix("duration-from-interval", 1);

    // spatial
    public static final FunctionIdentifier CREATE_POINT = FunctionConstants.newAsterix("create-point", 2);
    public static final FunctionIdentifier CREATE_LINE = FunctionConstants.newAsterix("create-line", 2);
    public static final FunctionIdentifier CREATE_POLYGON = FunctionConstants.newAsterix("create-polygon", 1);
    public static final FunctionIdentifier CREATE_CIRCLE = FunctionConstants.newAsterix("create-circle", 2);
    public static final FunctionIdentifier CREATE_RECTANGLE = FunctionConstants.newAsterix("create-rectangle", 2);
    public static final FunctionIdentifier SPATIAL_INTERSECT = FunctionConstants.newAsterix("spatial-intersect", 2);
    public static final FunctionIdentifier SPATIAL_AREA = FunctionConstants.newAsterix("spatial-area", 1);
    public static final FunctionIdentifier SPATIAL_DISTANCE = FunctionConstants.newAsterix("spatial-distance", 2);
    public static final FunctionIdentifier CREATE_MBR = FunctionConstants.newAsterix("create-mbr", 3);
    public static final FunctionIdentifier SPATIAL_CELL = FunctionConstants.newAsterix("spatial-cell", 4);
    public static final FunctionIdentifier SWITCH_CASE =
            FunctionConstants.newAsterix("switch-case", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier SLEEP = FunctionConstants.newAsterix("sleep", 2);
    public static final FunctionIdentifier INJECT_FAILURE = FunctionConstants.newAsterix("inject-failure", 2);
    public static final FunctionIdentifier FLOW_RECORD = FunctionConstants.newAsterix("flow-object", 1);
    public static final FunctionIdentifier CAST_TYPE = FunctionConstants.newAsterix("cast", 1);
    public static final FunctionIdentifier CAST_TYPE_LAX = FunctionConstants.newAsterix("cast-lax", 1);
    public static final FunctionIdentifier REFERENCE_TILE = FunctionConstants.newAsterix("reference-tile", 6);
    public static final FunctionIdentifier GET_INTERSECTION = FunctionConstants.newAsterix("get-intersection", 2);

    public static final FunctionIdentifier CREATE_UUID = FunctionConstants.newAsterix("create-uuid", 0);
    public static final FunctionIdentifier UUID = FunctionConstants.newAsterix("uuid", 0);
    public static final FunctionIdentifier CREATE_QUERY_UID = FunctionConstants.newAsterix("create-query-uid", 0);
    public static final FunctionIdentifier RANDOM = FunctionConstants.newAsterix("random", 0);
    public static final FunctionIdentifier RANDOM_WITH_SEED = FunctionConstants.newAsterix("random", 1);

    //Geo
    public static final FunctionIdentifier ST_AREA = FunctionConstants.newAsterix("st-area", 1);
    public static final FunctionIdentifier ST_MAKE_POINT = FunctionConstants.newAsterix("st-make-point", 2);
    public static final FunctionIdentifier ST_MAKE_POINT3D = FunctionConstants.newAsterix("st-make-point", 3);
    public static final FunctionIdentifier ST_MAKE_POINT3D_M = FunctionConstants.newAsterix("st-make-point", 4);
    public static final FunctionIdentifier ST_INTERSECTS = FunctionConstants.newAsterix("st-intersects", 2);
    public static final FunctionIdentifier ST_UNION = FunctionConstants.newAsterix("st-union", 2);
    public static final FunctionIdentifier ST_IS_COLLECTION = FunctionConstants.newAsterix("st-is-collection", 1);
    public static final FunctionIdentifier ST_CONTAINS = FunctionConstants.newAsterix("st-contains", 2);
    public static final FunctionIdentifier ST_CROSSES = FunctionConstants.newAsterix("st-crosses", 2);
    public static final FunctionIdentifier ST_DISJOINT = FunctionConstants.newAsterix("st-disjoint", 2);
    public static final FunctionIdentifier ST_EQUALS = FunctionConstants.newAsterix("st-equals", 2);
    public static final FunctionIdentifier ST_OVERLAPS = FunctionConstants.newAsterix("st-overlaps", 2);
    public static final FunctionIdentifier ST_TOUCHES = FunctionConstants.newAsterix("st-touches", 2);
    public static final FunctionIdentifier ST_WITHIN = FunctionConstants.newAsterix("st-within", 2);
    public static final FunctionIdentifier ST_IS_EMPTY = FunctionConstants.newAsterix("st-is-empty", 1);
    public static final FunctionIdentifier ST_IS_SIMPLE = FunctionConstants.newAsterix("st-is-simple", 1);
    public static final FunctionIdentifier ST_COORD_DIM = FunctionConstants.newAsterix("st-coord-dim", 1);
    public static final FunctionIdentifier ST_DIMENSION = FunctionConstants.newAsterix("st-dimension", 1);
    public static final FunctionIdentifier GEOMETRY_TYPE = FunctionConstants.newAsterix("geometry-type", 1);
    public static final FunctionIdentifier ST_M = FunctionConstants.newAsterix("st-m", 1);
    public static final FunctionIdentifier ST_N_RINGS = FunctionConstants.newAsterix("st-n-rings", 1);
    public static final FunctionIdentifier ST_N_POINTS = FunctionConstants.newAsterix("st-n-points", 1);
    public static final FunctionIdentifier ST_NUM_GEOMETRIIES = FunctionConstants.newAsterix("st-num-geometries", 1);
    public static final FunctionIdentifier ST_NUM_INTERIOR_RINGS =
            FunctionConstants.newAsterix("st-num-interior-rings", 1);
    public static final FunctionIdentifier ST_SRID = FunctionConstants.newAsterix("st-srid", 1);
    public static final FunctionIdentifier ST_X = FunctionConstants.newAsterix("st-x", 1);
    public static final FunctionIdentifier ST_Y = FunctionConstants.newAsterix("st-y", 1);
    public static final FunctionIdentifier ST_X_MAX = FunctionConstants.newAsterix("st-x-max", 1);
    public static final FunctionIdentifier ST_X_MIN = FunctionConstants.newAsterix("st-x-min", 1);
    public static final FunctionIdentifier ST_Y_MAX = FunctionConstants.newAsterix("st-y-max", 1);
    public static final FunctionIdentifier ST_Y_MIN = FunctionConstants.newAsterix("st-y-min", 1);
    public static final FunctionIdentifier ST_Z = FunctionConstants.newAsterix("st-z", 1);
    public static final FunctionIdentifier ST_Z_MIN = FunctionConstants.newAsterix("st-z-min", 1);
    public static final FunctionIdentifier ST_Z_MAX = FunctionConstants.newAsterix("st-z-max", 1);
    public static final FunctionIdentifier ST_AS_BINARY = FunctionConstants.newAsterix("st-as-binary", 1);
    public static final FunctionIdentifier ST_AS_TEXT = FunctionConstants.newAsterix("st-as-text", 1);
    public static final FunctionIdentifier ST_AS_GEOJSON = FunctionConstants.newAsterix("st-as-geojson", 1);
    public static final FunctionIdentifier ST_DISTANCE = FunctionConstants.newAsterix("st-distance", 2);
    public static final FunctionIdentifier ST_LENGTH = FunctionConstants.newAsterix("st-length", 1);
    public static final FunctionIdentifier SCALAR_ST_UNION_AGG = FunctionConstants.newAsterix("st_union", 1);
    public static final FunctionIdentifier SCALAR_ST_UNION_AGG_DISTINCT =
            FunctionConstants.newAsterix("st_union-distinct", 1);
    public static final FunctionIdentifier ST_UNION_AGG = FunctionConstants.newAsterix("agg-st_union", 1);
    public static final FunctionIdentifier ST_UNION_AGG_DISTINCT =
            FunctionConstants.newAsterix("agg-st_union-distinct", 1);
    public static final FunctionIdentifier SCALAR_ST_UNION_SQL_AGG = FunctionConstants.newAsterix("sql-st_union", 1);
    public static final FunctionIdentifier SCALAR_ST_UNION_SQL_AGG_DISTINCT =
            FunctionConstants.newAsterix("sql-st_union-distinct", 1);
    public static final FunctionIdentifier ST_UNION_SQL_AGG = FunctionConstants.newAsterix("agg-sql-st_union", 1);
    public static final FunctionIdentifier ST_UNION_SQL_AGG_DISTINCT =
            FunctionConstants.newAsterix("agg-sql-st_union-distinct", 1);

    public static final FunctionIdentifier ST_GEOM_FROM_TEXT = FunctionConstants.newAsterix("st-geom-from-text", 1);
    public static final FunctionIdentifier ST_GEOM_FROM_TEXT_SRID =
            FunctionConstants.newAsterix("st-geom-from-text", 2);
    public static final FunctionIdentifier ST_GEOM_FROM_WKB = FunctionConstants.newAsterix("st-geom-from-wkb", 1);
    public static final FunctionIdentifier ST_GEOM_FROM_WKB_SRID = FunctionConstants.newAsterix("st-geom-from-wkb", 2);
    public static final FunctionIdentifier ST_LINE_FROM_MULTIPOINT =
            FunctionConstants.newAsterix("st-line-from-multipoint", 1);
    public static final FunctionIdentifier ST_MAKE_ENVELOPE = FunctionConstants.newAsterix("st-make-envelope", 5);
    public static final FunctionIdentifier ST_IS_CLOSED = FunctionConstants.newAsterix("st-is-closed", 1);
    public static final FunctionIdentifier ST_IS_RING = FunctionConstants.newAsterix("st-is-ring", 1);
    public static final FunctionIdentifier ST_RELATE = FunctionConstants.newAsterix("st-relate", 3);
    public static final FunctionIdentifier ST_BOUNDARY = FunctionConstants.newAsterix("st-boundary", 1);
    public static final FunctionIdentifier ST_END_POINT = FunctionConstants.newAsterix("st-end-point", 1);
    public static final FunctionIdentifier ST_ENVELOPE = FunctionConstants.newAsterix("st-envelope", 1);
    public static final FunctionIdentifier ST_EXTERIOR_RING = FunctionConstants.newAsterix("st-exterior-ring", 1);
    public static final FunctionIdentifier ST_GEOMETRY_N = FunctionConstants.newAsterix("st-geometry-n", 2);
    public static final FunctionIdentifier ST_INTERIOR_RING_N = FunctionConstants.newAsterix("st-interior-ring-n", 2);
    public static final FunctionIdentifier ST_POINT_N = FunctionConstants.newAsterix("st-point-n", 2);
    public static final FunctionIdentifier ST_START_POINT = FunctionConstants.newAsterix("st-start-point", 1);
    public static final FunctionIdentifier ST_DIFFERENCE = FunctionConstants.newAsterix("st-difference", 2);
    public static final FunctionIdentifier ST_INTERSECTION = FunctionConstants.newAsterix("st-intersection", 2);
    public static final FunctionIdentifier ST_SYM_DIFFERENCE = FunctionConstants.newAsterix("st-sym-difference", 2);
    public static final FunctionIdentifier ST_POLYGONIZE = FunctionConstants.newAsterix("st-polygonize", 1);

    public static final FunctionIdentifier ST_MBR = FunctionConstants.newAsterix("st-mbr", 1);
    public static final FunctionIdentifier ST_MBR_ENLARGE = FunctionConstants.newAsterix("st-mbr-enlarge", 2);

    // Spatial and temporal type accessors
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_YEAR = FunctionConstants.newAsterix("get-year", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MONTH = FunctionConstants.newAsterix("get-month", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_DAY = FunctionConstants.newAsterix("get-day", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_HOUR = FunctionConstants.newAsterix("get-hour", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MIN = FunctionConstants.newAsterix("get-minute", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_SEC = FunctionConstants.newAsterix("get-second", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MILLISEC =
            FunctionConstants.newAsterix("get-millisecond", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START =
            FunctionConstants.newAsterix("get-interval-start", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END =
            FunctionConstants.newAsterix("get-interval-end", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_DATETIME =
            FunctionConstants.newAsterix("get-interval-start-datetime", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_DATETIME =
            FunctionConstants.newAsterix("get-interval-end-datetime", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_DATE =
            FunctionConstants.newAsterix("get-interval-start-date", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_DATE =
            FunctionConstants.newAsterix("get-interval-end-date", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_TIME =
            FunctionConstants.newAsterix("get-interval-start-time", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_TIME =
            FunctionConstants.newAsterix("get-interval-end-time", 1);
    public static final FunctionIdentifier INTERVAL_BIN = FunctionConstants.newAsterix("interval-bin", 3);
    public static final FunctionIdentifier OVERLAP_BINS = FunctionConstants.newAsterix("overlap-bins", 3);
    public static final FunctionIdentifier GET_OVERLAPPING_INTERVAL =
            FunctionConstants.newAsterix("get-overlapping-interval", 2);

    // Temporal functions
    public static final FunctionIdentifier UNIX_TIME_FROM_DATE_IN_DAYS =
            FunctionConstants.newAsterix("unix-time-from-date-in-days", 1);

    public static final FunctionIdentifier UNIX_TIME_FROM_DATE_IN_MS =
            FunctionConstants.newAsterix("unix-time-from-date-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_TIME_IN_MS =
            FunctionConstants.newAsterix("unix-time-from-time-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_MS =
            FunctionConstants.newAsterix("unix-time-from-datetime-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_MS_WITH_TZ =
            FunctionConstants.newAsterix("unix-time-from-datetime-in-ms", 2);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_SECS =
            FunctionConstants.newAsterix("unix-time-from-datetime-in-secs", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_SECS_WITH_TZ =
            FunctionConstants.newAsterix("unix-time-from-datetime-in-secs", 2);
    public static final FunctionIdentifier DATE_FROM_UNIX_TIME_IN_DAYS =
            FunctionConstants.newAsterix("date-from-unix-time-in-days", 1);
    public static final FunctionIdentifier DATE_FROM_DATETIME =
            FunctionConstants.newAsterix("get-date-from-datetime", 1);
    public static final FunctionIdentifier TIME_FROM_UNIX_TIME_IN_MS =
            FunctionConstants.newAsterix("time-from-unix-time-in-ms", 1);
    public static final FunctionIdentifier TIME_FROM_DATETIME =
            FunctionConstants.newAsterix("get-time-from-datetime", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_MS =
            FunctionConstants.newAsterix("datetime-from-unix-time-in-ms", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_MS_WITH_TZ =
            FunctionConstants.newAsterix("datetime-from-unix-time-in-ms", 2);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_SECS =
            FunctionConstants.newAsterix("datetime-from-unix-time-in-secs", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_SECS_WITH_TZ =
            FunctionConstants.newAsterix("datetime-from-unix-time-in-secs", 2);
    public static final FunctionIdentifier DATETIME_FROM_DATE_TIME =
            FunctionConstants.newAsterix("datetime-from-date-time", 2);
    public static final FunctionIdentifier CALENDAR_DURATION_FROM_DATETIME =
            FunctionConstants.newAsterix("calendar-duration-from-datetime", 2);
    public static final FunctionIdentifier CALENDAR_DURATION_FROM_DATE =
            FunctionConstants.newAsterix("calendar-duration-from-date", 2);
    public static final FunctionIdentifier ADJUST_TIME_FOR_TIMEZONE =
            FunctionConstants.newAsterix("adjust-time-for-timezone", 2);
    public static final FunctionIdentifier ADJUST_DATETIME_FOR_TIMEZONE =
            FunctionConstants.newAsterix("adjust-datetime-for-timezone", 2);
    public static final FunctionIdentifier DAY_OF_WEEK = FunctionConstants.newAsterix("day-of-week", 1);
    public static final FunctionIdentifier DAY_OF_WEEK2 = FunctionConstants.newAsterix("day-of-week", 2);
    public static final FunctionIdentifier DAY_OF_YEAR = FunctionConstants.newAsterix("day-of-year", 1);
    public static final FunctionIdentifier QUARTER_OF_YEAR = FunctionConstants.newAsterix("quarter-of-year", 1);
    public static final FunctionIdentifier WEEK_OF_YEAR = FunctionConstants.newAsterix("week-of-year", 1);
    public static final FunctionIdentifier WEEK_OF_YEAR2 = FunctionConstants.newAsterix("week-of-year", 2);
    public static final FunctionIdentifier PARSE_DATE = FunctionConstants.newAsterix("parse-date", 2);
    public static final FunctionIdentifier PARSE_TIME = FunctionConstants.newAsterix("parse-time", 2);
    public static final FunctionIdentifier PARSE_DATETIME = FunctionConstants.newAsterix("parse-datetime", 2);
    public static final FunctionIdentifier PRINT_DATE = FunctionConstants.newAsterix("print-date", 2);
    public static final FunctionIdentifier PRINT_TIME = FunctionConstants.newAsterix("print-time", 2);
    public static final FunctionIdentifier PRINT_DATETIME = FunctionConstants.newAsterix("print-datetime", 2);

    public static final FunctionIdentifier GET_POINT_X_COORDINATE_ACCESSOR = FunctionConstants.newAsterix("get-x", 1);
    public static final FunctionIdentifier GET_POINT_Y_COORDINATE_ACCESSOR = FunctionConstants.newAsterix("get-y", 1);
    public static final FunctionIdentifier GET_CIRCLE_RADIUS_ACCESSOR = FunctionConstants.newAsterix("get-radius", 1);
    public static final FunctionIdentifier GET_CIRCLE_CENTER_ACCESSOR = FunctionConstants.newAsterix("get-center", 1);
    public static final FunctionIdentifier GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR =
            FunctionConstants.newAsterix("get-points", 1);

    public static final FunctionIdentifier EQ = AlgebricksBuiltinFunctions.EQ;
    public static final FunctionIdentifier LE = AlgebricksBuiltinFunctions.LE;
    public static final FunctionIdentifier GE = AlgebricksBuiltinFunctions.GE;
    public static final FunctionIdentifier LT = AlgebricksBuiltinFunctions.LT;
    public static final FunctionIdentifier GT = AlgebricksBuiltinFunctions.GT;
    public static final FunctionIdentifier NEQ = AlgebricksBuiltinFunctions.NEQ;
    public static final FunctionIdentifier AND = AlgebricksBuiltinFunctions.AND;
    public static final FunctionIdentifier OR = AlgebricksBuiltinFunctions.OR;
    public static final FunctionIdentifier NOT = AlgebricksBuiltinFunctions.NOT;
    public static final FunctionIdentifier NUMERIC_ADD = AlgebricksBuiltinFunctions.NUMERIC_ADD;
    public static final FunctionIdentifier IS_MISSING = AlgebricksBuiltinFunctions.IS_MISSING;
    public static final FunctionIdentifier IS_NULL = AlgebricksBuiltinFunctions.IS_NULL;
    public static final FunctionIdentifier IS_UNKNOWN = FunctionConstants.newAsterix("is-unknown", 1);
    public static final FunctionIdentifier IS_ATOMIC = FunctionConstants.newAsterix("is-atomic", 1);
    public static final FunctionIdentifier IS_BOOLEAN = FunctionConstants.newAsterix("is-boolean", 1);
    public static final FunctionIdentifier IS_BINARY = FunctionConstants.newAsterix("is-binary", 1);
    public static final FunctionIdentifier IS_POINT = FunctionConstants.newAsterix("is-point", 1);
    public static final FunctionIdentifier IS_LINE = FunctionConstants.newAsterix("is-line", 1);
    public static final FunctionIdentifier IS_RECTANGLE = FunctionConstants.newAsterix("is-rectangle", 1);
    public static final FunctionIdentifier IS_CIRCLE = FunctionConstants.newAsterix("is-circle", 1);
    public static final FunctionIdentifier IS_POLYGON = FunctionConstants.newAsterix("is-polygon", 1);
    public static final FunctionIdentifier IS_SPATIAL = FunctionConstants.newAsterix("is-spatial", 1);
    public static final FunctionIdentifier IS_DATE = FunctionConstants.newAsterix("is-date", 1);
    public static final FunctionIdentifier IS_DATETIME = FunctionConstants.newAsterix("is-datetime", 1);
    public static final FunctionIdentifier IS_TIME = FunctionConstants.newAsterix("is-time", 1);
    public static final FunctionIdentifier IS_DURATION = FunctionConstants.newAsterix("is-duration", 1);
    public static final FunctionIdentifier IS_INTERVAL = FunctionConstants.newAsterix("is-interval", 1);
    public static final FunctionIdentifier IS_TEMPORAL = FunctionConstants.newAsterix("is-temporal", 1);
    public static final FunctionIdentifier IS_UUID = FunctionConstants.newAsterix("is-uuid", 1);
    public static final FunctionIdentifier IS_NUMBER = FunctionConstants.newAsterix("is-number", 1);
    public static final FunctionIdentifier IS_STRING = FunctionConstants.newAsterix("is-string", 1);
    public static final FunctionIdentifier IS_ARRAY = FunctionConstants.newAsterix("is-array", 1);
    public static final FunctionIdentifier IS_OBJECT = FunctionConstants.newAsterix("is-object", 1);
    public static final FunctionIdentifier IS_MULTISET = FunctionConstants.newAsterix("is-multiset", 1);
    public static final FunctionIdentifier GET_TYPE = FunctionConstants.newAsterix("get-type", 1);

    public static final FunctionIdentifier IS_SYSTEM_NULL = FunctionConstants.newAsterix("is-system-null", 1);
    public static final FunctionIdentifier CHECK_UNKNOWN = FunctionConstants.newAsterix("check-unknown", 1);
    public static final FunctionIdentifier COLLECTION_TO_SEQUENCE =
            FunctionConstants.newAsterix("collection-to-sequence", 1);

    public static final FunctionIdentifier IF_MISSING =
            FunctionConstants.newAsterix("if-missing", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_NULL =
            FunctionConstants.newAsterix("if-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_MISSING_OR_NULL =
            FunctionConstants.newAsterix("if-missing-or-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_SYSTEM_NULL =
            FunctionConstants.newAsterix("if-system-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_INF = FunctionConstants.newAsterix("if-inf", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_NAN = FunctionConstants.newAsterix("if-nan", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_NAN_OR_INF =
            FunctionConstants.newAsterix("if-nan-or-inf", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier MISSING_IF = FunctionConstants.newAsterix("missing-if", 2);
    public static final FunctionIdentifier NULL_IF = FunctionConstants.newAsterix("null-if", 2);
    public static final FunctionIdentifier NAN_IF = FunctionConstants.newAsterix("nan-if", 2);
    public static final FunctionIdentifier POSINF_IF = FunctionConstants.newAsterix("posinf-if", 2);
    public static final FunctionIdentifier NEGINF_IF = FunctionConstants.newAsterix("neginf-if", 2);

    public static final FunctionIdentifier TO_ATOMIC = FunctionConstants.newAsterix("to-atomic", 1);
    public static final FunctionIdentifier TO_ARRAY = FunctionConstants.newAsterix("to-array", 1);
    public static final FunctionIdentifier TO_BIGINT = FunctionConstants.newAsterix("to-bigint", 1);
    public static final FunctionIdentifier TO_BOOLEAN = FunctionConstants.newAsterix("to-boolean", 1);
    public static final FunctionIdentifier TO_DOUBLE = FunctionConstants.newAsterix("to-double", 1);
    public static final FunctionIdentifier TO_NUMBER = FunctionConstants.newAsterix("to-number", 1);
    public static final FunctionIdentifier TO_OBJECT = FunctionConstants.newAsterix("to-object", 1);
    public static final FunctionIdentifier TO_STRING = FunctionConstants.newAsterix("to-string", 1);

    public static final FunctionIdentifier TREAT_AS_INTEGER = FunctionConstants.newAsterix("treat-as-integer", 1);
    public static final FunctionIdentifier IS_NUMERIC_ADD_COMPATIBLE =
            FunctionConstants.newAsterix("is-numeric-add-compatible", 1);

    public static final FunctionIdentifier EXTERNAL_LOOKUP =
            FunctionConstants.newAsterix("external-lookup", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier GET_JOB_PARAMETER = FunctionConstants.newAsterix("get-job-param", 1);

    public static final FunctionIdentifier META = FunctionConstants.newAsterix("meta", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier META_KEY =
            FunctionConstants.newAsterix("meta-key", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier DECODE_DATAVERSE_NAME =
            FunctionConstants.newAsterix("decode-dataverse-name", 1);

    public static final FunctionIdentifier SERIALIZED_SIZE = FunctionConstants.newAsterix("serialized-size", 1);

    static {
        // first, take care of Algebricks builtin functions
        addFunction(IS_MISSING, BooleanOnlyTypeComputer.INSTANCE, true);
        addFunction(IS_UNKNOWN, BooleanOnlyTypeComputer.INSTANCE, true);
        addFunction(IS_NULL, BooleanOrMissingTypeComputer.INSTANCE, true);
        addFunction(IS_SYSTEM_NULL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_ATOMIC, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_BOOLEAN, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_BINARY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_POINT, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_LINE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_RECTANGLE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_CIRCLE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_POLYGON, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_SPATIAL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_DATE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_DATETIME, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_TIME, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_DURATION, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_INTERVAL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_TEMPORAL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_UUID, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_NUMBER, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_STRING, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_ARRAY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_OBJECT, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_MULTISET, ABooleanTypeComputer.INSTANCE, true);
        addFunction(NOT, ABooleanTypeComputer.INSTANCE, true);

        addFunction(GET_TYPE, AStringTypeComputer.INSTANCE, true);

        addPrivateFunction(EQ, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(LE, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(GE, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(LT, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(GT, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(AND, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(NEQ, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(OR, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_ADD, NumericAddSubMulDivTypeComputer.INSTANCE_ADD, true);

        // deep equality
        addFunction(DEEP_EQUAL, BooleanFunctionTypeComputer.INSTANCE, true);

        // and then, Asterix builtin functions
        addPrivateFunction(CHECK_UNKNOWN, NotUnknownTypeComputer.INSTANCE, true);
        addPrivateFunction(ANY_COLLECTION_MEMBER, CollectionMemberResultType.INSTANCE_MISSABLE, true);
        addFunction(BOOLEAN_CONSTRUCTOR, ABooleanTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(CIRCLE_CONSTRUCTOR, ACircleTypeComputer.INSTANCE, true);
        addPrivateFunction(CONCAT_NON_NULL, ConcatNonNullTypeComputer.INSTANCE, true);
        addFunction(GROUPING, AInt64TypeComputer.INSTANCE, true);

        addPrivateFunction(COUNTHASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(COUNTHASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE, true);
        addFunction(CREATE_CIRCLE, ACircleTypeComputer.INSTANCE, true);
        addFunction(CREATE_LINE, ALineTypeComputer.INSTANCE, true);
        addPrivateFunction(CREATE_MBR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(CREATE_POINT, APointTypeComputer.INSTANCE, true);
        addFunction(CREATE_POLYGON, APolygonTypeComputer.INSTANCE, true);
        addFunction(CREATE_RECTANGLE, ARectangleTypeComputer.INSTANCE, true);
        addFunction(CREATE_UUID, AUUIDTypeComputer.INSTANCE, false);
        addFunction(UUID, AUUIDTypeComputer.INSTANCE, false);
        addPrivateFunction(CREATE_QUERY_UID, ABinaryTypeComputer.INSTANCE, false);
        addFunction(UUID_CONSTRUCTOR, AUUIDTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(RANDOM, ADoubleTypeComputer.INSTANCE, false);
        addFunction(RANDOM_WITH_SEED, NumericUnaryTypeComputer.INSTANCE_DOUBLE, false);

        addFunction(DATE_CONSTRUCTOR, ADateTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DATE_CONSTRUCTOR_WITH_FORMAT, ADateTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DATETIME_CONSTRUCTOR, ADateTimeTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DATETIME_CONSTRUCTOR_WITH_FORMAT, ADateTimeTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DOUBLE_CONSTRUCTOR, ADoubleTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DURATION_CONSTRUCTOR, ADurationTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(YEAR_MONTH_DURATION_CONSTRUCTOR, AYearMonthDurationTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DAY_TIME_DURATION_CONSTRUCTOR, ADayTimeDurationTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(EDIT_DISTANCE, AInt64TypeComputer.INSTANCE, true);
        addFunction(EDIT_DISTANCE_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addPrivateFunction(EDIT_DISTANCE_STRING_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(EDIT_DISTANCE_LIST_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(EMPTY_STREAM, ABooleanTypeComputer.INSTANCE, true);

        addFunction(FLOAT_CONSTRUCTOR, AFloatTypeComputer.INSTANCE_NULLABLE, true);
        addPrivateFunction(FUZZY_EQ, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(GET_HANDLE, AnyTypeComputer.INSTANCE, true);
        addPrivateFunction(GET_ITEM, NonTaggedGetItemResultType.INSTANCE, true);
        addPrivateFunction(GET_DATA, AnyTypeComputer.INSTANCE, true);
        addPrivateFunction(GRAM_TOKENS, OrderedListOfAStringTypeComputer.INSTANCE, true);
        addPrivateFunction(HASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(HASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(INDEX_SEARCH, AnyTypeComputer.INSTANCE, true);
        addFunction(INT8_CONSTRUCTOR, AInt8TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(INT16_CONSTRUCTOR, AInt16TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(INT32_CONSTRUCTOR, AInt32TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(INT64_CONSTRUCTOR, AInt64TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(LEN, AInt64TypeComputer.INSTANCE, true);
        addFunction(LINE_CONSTRUCTOR, ALineTypeComputer.INSTANCE, true);
        addPrivateFunction(MAKE_FIELD_INDEX_HANDLE, AnyTypeComputer.INSTANCE, true);
        addPrivateFunction(MAKE_FIELD_NAME_HANDLE, AnyTypeComputer.INSTANCE, true);

        // cast null type constructors
        addFunction(BOOLEAN_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_BOOLEAN, true);
        addFunction(INT8_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_INT8, true);
        addFunction(INT16_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_INT16, true);
        addFunction(INT32_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_INT32, true);
        addFunction(INT64_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_INT64, true);
        addFunction(FLOAT_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_FLOAT, true);
        addFunction(DOUBLE_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(STRING_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_STRING, true);
        addFunction(DATE_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_DATE, true);
        addFunction(DATE_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT, NullableTypeComputer.INSTANCE_DATE, true);
        addFunction(TIME_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_TIME, true);
        addFunction(TIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT, NullableTypeComputer.INSTANCE_TIME, true);
        addFunction(DATETIME_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_DATE_TIME, true);
        addFunction(DATETIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT, NullableTypeComputer.INSTANCE_DATE_TIME, true);
        addFunction(DURATION_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_DURATION, true);
        addFunction(DAY_TIME_DURATION_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_DAY_TIME_DURATION, true);
        addFunction(YEAR_MONTH_DURATION_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_YEAR_MONTH_DURATION,
                true);
        addFunction(UUID_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_UUID, true);
        addFunction(BINARY_BASE64_DEFAULT_NULL_CONSTRUCTOR, NullableTypeComputer.INSTANCE_BINARY, true);

        addPrivateFunction(NUMERIC_UNARY_MINUS, NumericUnaryTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_SUBTRACT, NumericAddSubMulDivTypeComputer.INSTANCE_SUB, true);
        addPrivateFunction(NUMERIC_MULTIPLY, NumericAddSubMulDivTypeComputer.INSTANCE_MUL_POW, true);
        addPrivateFunction(NUMERIC_DIVIDE, NumericDivideTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_MOD, NumericAddSubMulDivTypeComputer.INSTANCE_DIV_MOD, true);
        addPrivateFunction(NUMERIC_DIV, NumericAddSubMulDivTypeComputer.INSTANCE_DIV_MOD, true);
        addFunction(NUMERIC_ABS, NumericUnaryTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ACOS, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_ASIN, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_ATAN, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_ATAN2, NumericBinaryToDoubleTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_DEGREES, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_RADIANS, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_COS, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_COSH, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_SIN, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_SINH, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_TAN, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_TANH, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_E, ADoubleTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_EXP, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_LN, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_LOG, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_PI, ADoubleTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_POWER, NumericAddSubMulDivTypeComputer.INSTANCE_MUL_POW, true);
        addFunction(NUMERIC_SQRT, NumericUnaryTypeComputer.INSTANCE_DOUBLE, true);
        addFunction(NUMERIC_SIGN, NumericUnaryTypeComputer.INSTANCE_INT8, true);
        addFunction(NUMERIC_CEILING, NumericUnaryTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_FLOOR, NumericUnaryTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND, NumericRoundTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_WITH_ROUND_DIGIT, NumericRoundTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN, NumericUnaryTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN2, NumericRoundTypeComputer.INSTANCE_ROUND_HF_TRUNC, true);
        addFunction(NUMERIC_ROUND_HALF_UP2, NumericRoundTypeComputer.INSTANCE_ROUND_HF_TRUNC, true);
        addFunction(NUMERIC_TRUNC, NumericRoundTypeComputer.INSTANCE_ROUND_HF_TRUNC, true);

        addFunction(BINARY_LENGTH, UnaryBinaryInt64TypeComputer.INSTANCE, true);
        addFunction(PARSE_BINARY, ABinaryTypeComputer.INSTANCE, true);
        addFunction(PRINT_BINARY, AStringTypeComputer.INSTANCE, true);
        addFunction(BINARY_CONCAT, ConcatTypeComputer.INSTANCE_BINARY, true);
        addFunction(SUBBINARY_FROM, ABinaryTypeComputer.INSTANCE, true);
        addFunction(SUBBINARY_FROM_TO, ABinaryTypeComputer.INSTANCE, true);
        addFunction(FIND_BINARY, AInt64TypeComputer.INSTANCE, true);
        addFunction(FIND_BINARY_FROM, AInt64TypeComputer.INSTANCE, true);

        addFunction(BIT_AND, BitMultipleValuesTypeComputer.INSTANCE_INT64, true);
        addFunction(BIT_OR, BitMultipleValuesTypeComputer.INSTANCE_INT64, true);
        addFunction(BIT_XOR, BitMultipleValuesTypeComputer.INSTANCE_INT64, true);
        addFunction(BIT_NOT, BitMultipleValuesTypeComputer.INSTANCE_INT64, true);
        addFunction(BIT_COUNT, BitMultipleValuesTypeComputer.INSTANCE_INT32, true);
        addFunction(BIT_SET, BitValuePositionFlagTypeComputer.INSTANCE_SET_CLEAR, true);
        addFunction(BIT_CLEAR, BitValuePositionFlagTypeComputer.INSTANCE_SET_CLEAR, true);
        addFunction(BIT_SHIFT_WITHOUT_ROTATE_FLAG, BitValuePositionFlagTypeComputer.INSTANCE_SHIFT_WITHOUT_FLAG, true);
        addFunction(BIT_SHIFT_WITH_ROTATE_FLAG, BitValuePositionFlagTypeComputer.INSTANCE_SHIFT_WITH_FLAG, true);
        addFunction(BIT_TEST_WITHOUT_ALL_FLAG, BitValuePositionFlagTypeComputer.INSTANCE_TEST_WITHOUT_FLAG, true);
        addFunction(BIT_TEST_WITH_ALL_FLAG, BitValuePositionFlagTypeComputer.INSTANCE_TEST_WITH_FLAG, true);
        addFunction(IS_BIT_SET_WITHOUT_ALL_FLAG, BitValuePositionFlagTypeComputer.INSTANCE_TEST_WITHOUT_FLAG, true);
        addFunction(IS_BIT_SET_WITH_ALL_FLAG, BitValuePositionFlagTypeComputer.INSTANCE_TEST_WITH_FLAG, true);

        // string functions
        addFunction(STRING_CONSTRUCTOR, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_LIKE, BooleanFunctionTypeComputer.INSTANCE, true);
        addFunction(STRING_CONTAINS, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_TO_CODEPOINT, UniformInputTypeComputer.STRING_INT64_LIST_INSTANCE, true);
        addFunction(CODEPOINT_TO_STRING, Int64ArrayToStringTypeComputer.INSTANCE, true);
        addFunction(STRING_CONCAT, ConcatTypeComputer.INSTANCE_STRING, true);
        addFunction(SUBSTRING, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(SUBSTRING_OFFSET_1, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(SUBSTRING2, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(SUBSTRING2_OFFSET_1, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_LENGTH, UniformInputTypeComputer.STRING_INT64_INSTANCE, true);
        addFunction(STRING_LOWERCASE, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_UPPERCASE, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_INITCAP, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_TRIM, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_LTRIM, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_RTRIM, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_TRIM2, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_LTRIM2, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_RTRIM2, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_POSITION, UniformInputTypeComputer.STRING_INT32_INSTANCE, true);
        addFunction(STRING_POSITION_OFFSET_1, UniformInputTypeComputer.STRING_INT32_INSTANCE, true);
        addFunction(STRING_STARTS_WITH, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_ENDS_WITH, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_MATCHES, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_MATCHES_WITH_FLAG, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_REGEXP_LIKE, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_REGEXP_LIKE_WITH_FLAG, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_REGEXP_POSITION, UniformInputTypeComputer.STRING_INT32_INSTANCE, true);
        addFunction(STRING_REGEXP_POSITION_OFFSET_1, UniformInputTypeComputer.STRING_INT32_INSTANCE, true);
        addFunction(STRING_REGEXP_POSITION_WITH_FLAG, UniformInputTypeComputer.STRING_INT32_INSTANCE, true);
        addFunction(STRING_REGEXP_POSITION_OFFSET_1_WITH_FLAG, UniformInputTypeComputer.STRING_INT32_INSTANCE, true);
        addFunction(STRING_REGEXP_REPLACE, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_REGEXP_REPLACE_WITH_FLAG, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_REPLACE, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(STRING_REGEXP_MATCHES, UniformInputTypeComputer.STRING_STRING_LIST_INSTANCE, true);
        addFunction(STRING_REGEXP_SPLIT, UniformInputTypeComputer.STRING_STRING_LIST_INSTANCE, true);
        addFunction(STRING_REPLACE_WITH_LIMIT, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_REVERSE, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(SUBSTRING_BEFORE, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addFunction(SUBSTRING_AFTER, UniformInputTypeComputer.STRING_STRING_INSTANCE, true);
        addPrivateFunction(STRING_EQUAL, UniformInputTypeComputer.STRING_BOOLEAN_INSTANCE, true);
        addFunction(STRING_JOIN, StringJoinTypeComputer.INSTANCE, true);
        addFunction(STRING_REPEAT, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_SPLIT, UniformInputTypeComputer.STRING_STRING_LIST_INSTANCE, true);
        addFunction(STRING_PARSE_JSON, AnyTypeComputer.INSTANCE, true);

        addPrivateFunction(ORDERED_LIST_CONSTRUCTOR, OrderedListConstructorTypeComputer.INSTANCE, true);
        addFunction(POINT_CONSTRUCTOR, APointTypeComputer.INSTANCE, true);
        addFunction(POINT3D_CONSTRUCTOR, APoint3DTypeComputer.INSTANCE, true);
        addFunction(POLYGON_CONSTRUCTOR, APolygonTypeComputer.INSTANCE, true);
        addPrivateFunction(PREFIX_LEN_JACCARD, AInt32TypeComputer.INSTANCE, true);
        addFunction(RANGE, AInt64TypeComputer.INSTANCE, true);
        addFunction(RECTANGLE_CONSTRUCTOR, ARectangleTypeComputer.INSTANCE, true);

        addFunction(TO_ATOMIC, AnyTypeComputer.INSTANCE, true);
        addFunction(TO_ARRAY, ToArrayTypeComputer.INSTANCE, true);
        addFunction(TO_BIGINT, ToBigIntTypeComputer.INSTANCE, true);
        addFunction(TO_BOOLEAN, ABooleanTypeComputer.INSTANCE, true);
        addFunction(TO_DOUBLE, ToDoubleTypeComputer.INSTANCE, true);
        addFunction(TO_NUMBER, ToNumberTypeComputer.INSTANCE, true);
        addFunction(TO_OBJECT, ToObjectTypeComputer.INSTANCE, true);
        addFunction(TO_STRING, AStringTypeComputer.INSTANCE_NULLABLE, true);

        addPrivateFunction(TREAT_AS_INTEGER, TreatAsTypeComputer.INSTANCE_INTEGER, true);
        addPrivateFunction(IS_NUMERIC_ADD_COMPATIBLE, BooleanOnlyTypeComputer.INSTANCE, true);

        addFunction(IF_INF, IfNanOrInfTypeComputer.INSTANCE, true);
        addFunction(IF_MISSING, IfMissingTypeComputer.INSTANCE, true);
        addFunction(IF_MISSING_OR_NULL, IfMissingOrNullTypeComputer.INSTANCE, true);
        addFunction(IF_NULL, IfNullTypeComputer.INSTANCE, true);
        addFunction(IF_NAN, IfNanOrInfTypeComputer.INSTANCE_SKIP_MISSING, true);
        addFunction(IF_NAN_OR_INF, IfNanOrInfTypeComputer.INSTANCE_SKIP_MISSING, true);
        addPrivateFunction(IF_SYSTEM_NULL, IfNullTypeComputer.INSTANCE, true);

        addFunction(MISSING_IF, MissingIfTypeComputer.INSTANCE, true);
        addFunction(NULL_IF, NullIfTypeComputer.INSTANCE, true);
        addFunction(NAN_IF, DoubleIfTypeComputer.INSTANCE, true);
        addFunction(POSINF_IF, DoubleIfTypeComputer.INSTANCE, true);
        addFunction(NEGINF_IF, DoubleIfTypeComputer.INSTANCE, true);

        // Aggregate Functions
        ScalarVersionOfAggregateResultType scalarNumericSumTypeComputer =
                new ScalarVersionOfAggregateResultType(NumericSumAggTypeComputer.INSTANCE);
        ScalarVersionOfAggregateResultType scalarMinMaxTypeComputer =
                new ScalarVersionOfAggregateResultType(MinMaxAggTypeComputer.INSTANCE);
        ScalarVersionOfAggregateResultType scalarUnionMbrTypeComputer =
                new ScalarVersionOfAggregateResultType(UnionMbrAggTypeComputer.INSTANCE);

        addPrivateFunction(LISTIFY, OrderedListConstructorTypeComputer.INSTANCE, true);
        addFunction(SCALAR_ARRAYAGG, ScalarArrayAggTypeComputer.INSTANCE, true);
        addFunction(MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(NON_EMPTY_STREAM, ABooleanTypeComputer.INSTANCE, true);
        addFunction(COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SCALAR_FIRST_ELEMENT, CollectionMemberResultType.INSTANCE_NULLABLE, true);
        addPrivateFunction(SCALAR_LOCAL_FIRST_ELEMENT, CollectionMemberResultType.INSTANCE_NULLABLE, true);
        addPrivateFunction(SCALAR_LAST_ELEMENT, CollectionMemberResultType.INSTANCE_NULLABLE, true);
        addPrivateFunction(FIRST_ELEMENT, PropagateTypeComputer.INSTANCE_NULLABLE, true);
        addPrivateFunction(LOCAL_FIRST_ELEMENT, PropagateTypeComputer.INSTANCE_NULLABLE, true);
        addPrivateFunction(LAST_ELEMENT, PropagateTypeComputer.INSTANCE_NULLABLE, true);
        addPrivateFunction(LOCAL_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SAMPLING, ABinaryTypeComputer.INSTANCE, true);
        addPrivateFunction(RANGE_MAP, ABinaryTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(NULL_WRITER, PropagateTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);

        // SUM
        addFunction(SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SUM, scalarNumericSumTypeComputer, true);
        addPrivateFunction(LOCAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);

        addPrivateFunction(SERIAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(SCALAR_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_COUNT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SCALAR_MAX, scalarMinMaxTypeComputer, true);
        addFunction(SCALAR_MIN, scalarMinMaxTypeComputer, true);
        addPrivateFunction(INTERMEDIATE_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(SCALAR_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_UNION_MBR, scalarUnionMbrTypeComputer, true);

        // SQL SUM
        addFunction(SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_SUM, scalarNumericSumTypeComputer, true);
        addPrivateFunction(LOCAL_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);

        addFunction(SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(SQL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SQL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SQL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_MAX, scalarMinMaxTypeComputer, true);
        addFunction(SCALAR_SQL_MIN, scalarMinMaxTypeComputer, true);
        addPrivateFunction(INTERMEDIATE_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(SQL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_UNION_MBR, ARectangleTypeComputer.INSTANCE, true);

        addPrivateFunction(SERIAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_STDDEV_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_STDDEV_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_STDDEV_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_STDDEV_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_VAR_SAMP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_VAR_SAMP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_VAR_POP, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_VAR_POP, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SKEWNESS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SKEWNESS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_KURTOSIS, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_KURTOSIS, LocalSingleVarStatisticsTypeComputer.INSTANCE, true);

        // Distinct aggregate functions
        addFunction(LISTIFY_DISTINCT, OrderedListConstructorTypeComputer.INSTANCE, true);
        addFunction(SCALAR_ARRAYAGG_DISTINCT, ScalarArrayAggTypeComputer.INSTANCE, true);

        addFunction(COUNT_DISTINCT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SCALAR_COUNT_DISTINCT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SQL_COUNT_DISTINCT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_COUNT_DISTINCT, AInt64TypeComputer.INSTANCE, true);

        addFunction(SUM_DISTINCT, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SUM_DISTINCT, scalarNumericSumTypeComputer, true);
        addFunction(SQL_SUM_DISTINCT, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_SUM_DISTINCT, scalarNumericSumTypeComputer, true);

        addFunction(AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(MAX_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_MAX_DISTINCT, scalarMinMaxTypeComputer, true);
        addFunction(SQL_MAX_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_MAX_DISTINCT, scalarMinMaxTypeComputer, true);

        addFunction(MIN_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_MIN_DISTINCT, scalarMinMaxTypeComputer, true);
        addFunction(SQL_MIN_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_MIN_DISTINCT, scalarMinMaxTypeComputer, true);

        addFunction(STDDEV_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_STDDEV_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_STDDEV_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_STDDEV_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(STDDEV_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_STDDEV_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_STDDEV_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_STDDEV_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(VAR_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_VAR_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_VAR_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_VAR_SAMP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(VAR_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_VAR_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_VAR_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_VAR_POP_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(SKEWNESS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SKEWNESS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_SKEWNESS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_SKEWNESS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(KURTOSIS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_KURTOSIS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_KURTOSIS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_KURTOSIS_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        // Window functions

        addFunction(CUME_DIST, ADoubleTypeComputer.INSTANCE, false);
        addFunction(CUME_DIST_IMPL, ADoubleTypeComputer.INSTANCE, false);
        addFunction(DENSE_RANK, AInt64TypeComputer.INSTANCE, false);
        addFunction(DENSE_RANK_IMPL, AInt64TypeComputer.INSTANCE, false);
        addFunction(FIRST_VALUE, CollectionMemberResultType.INSTANCE_NULLABLE, false);
        addFunction(FIRST_VALUE_IMPL, CollectionMemberResultType.INSTANCE_NULLABLE, false);
        addFunction(LAG, AnyTypeComputer.INSTANCE, false);
        addFunction(LAG_IMPL, AnyTypeComputer.INSTANCE, false);
        addFunction(LAST_VALUE, CollectionMemberResultType.INSTANCE_NULLABLE, false);
        addFunction(LAST_VALUE_IMPL, CollectionMemberResultType.INSTANCE_NULLABLE, false);
        addFunction(LEAD, AnyTypeComputer.INSTANCE, false);
        addFunction(LEAD_IMPL, AnyTypeComputer.INSTANCE, false);
        addFunction(NTH_VALUE, CollectionMemberResultType.INSTANCE_NULLABLE, false);
        addFunction(NTH_VALUE_IMPL, CollectionMemberResultType.INSTANCE_NULLABLE, false);
        addFunction(NTILE, AInt64TypeComputer.INSTANCE_NULLABLE, false);
        addFunction(NTILE_IMPL, AInt64TypeComputer.INSTANCE_NULLABLE, false);
        addFunction(RANK, AInt64TypeComputer.INSTANCE, false);
        addFunction(RANK_IMPL, AInt64TypeComputer.INSTANCE, false);
        addFunction(RATIO_TO_REPORT, ADoubleTypeComputer.INSTANCE, false);
        addFunction(RATIO_TO_REPORT_IMPL, ADoubleTypeComputer.INSTANCE, false);
        addFunction(ROW_NUMBER, AInt64TypeComputer.INSTANCE, false);
        addFunction(ROW_NUMBER_IMPL, AInt64TypeComputer.INSTANCE, false);
        addFunction(PERCENT_RANK, ADoubleTypeComputer.INSTANCE, false);
        addFunction(PERCENT_RANK_IMPL, ADoubleTypeComputer.INSTANCE, false);
        addPrivateFunction(WIN_MARK_FIRST_MISSING_IMPL, ABooleanTypeComputer.INSTANCE, false);
        addPrivateFunction(WIN_MARK_FIRST_NULL_IMPL, ABooleanTypeComputer.INSTANCE, false);
        addPrivateFunction(WIN_PARTITION_LENGTH_IMPL, AInt64TypeComputer.INSTANCE, false);

        // Similarity functions
        addFunction(EDIT_DISTANCE_CONTAINS, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addFunction(SIMILARITY_JACCARD, AFloatTypeComputer.INSTANCE, true);
        addFunction(SIMILARITY_JACCARD_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_SORTED, AFloatTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_SORTED_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_PREFIX, AFloatTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_PREFIX_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);

        // Full-text function
        addFunction(FULLTEXT_CONTAINS, FullTextContainsResultTypeComputer.INSTANCE, true);
        addFunction(FULLTEXT_CONTAINS_WO_OPTION, FullTextContainsResultTypeComputer.INSTANCE, true);

        // Spatial functions
        addFunction(SPATIAL_AREA, ADoubleTypeComputer.INSTANCE, true);
        addFunction(SPATIAL_CELL, ARectangleTypeComputer.INSTANCE, true);
        addFunction(SPATIAL_DISTANCE, ADoubleTypeComputer.INSTANCE, true);
        addFunction(SPATIAL_INTERSECT, ABooleanTypeComputer.INSTANCE, true);
        addFunction(GET_POINT_X_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_POINT_Y_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_CIRCLE_RADIUS_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_CIRCLE_CENTER_ACCESSOR, APointTypeComputer.INSTANCE, true);
        addFunction(GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR, OrderedListOfAPointTypeComputer.INSTANCE, true);
        addPrivateFunction(SPATIAL_TILE, AInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(REFERENCE_TILE, AInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(GET_INTERSECTION, ARectangleTypeComputer.INSTANCE, true);

        //geo functions
        addFunction(ST_AREA, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_MAKE_POINT, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_MAKE_POINT3D, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_MAKE_POINT3D_M, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_INTERSECTS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_UNION, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_IS_COLLECTION, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_CONTAINS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_CROSSES, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_DISJOINT, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_EQUALS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_OVERLAPS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_TOUCHES, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_WITHIN, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_IS_EMPTY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_IS_SIMPLE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_IS_COLLECTION, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_COORD_DIM, AInt32TypeComputer.INSTANCE, true);
        addFunction(ST_DIMENSION, AInt32TypeComputer.INSTANCE, true);
        addFunction(GEOMETRY_TYPE, AStringTypeComputer.INSTANCE, true);
        addFunction(ST_M, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_N_RINGS, AInt32TypeComputer.INSTANCE, true);
        addFunction(ST_N_POINTS, AInt32TypeComputer.INSTANCE, true);
        addFunction(ST_NUM_GEOMETRIIES, AInt32TypeComputer.INSTANCE, true);
        addFunction(ST_NUM_INTERIOR_RINGS, AInt32TypeComputer.INSTANCE, true);
        addFunction(ST_SRID, AInt32TypeComputer.INSTANCE, true);
        addFunction(ST_X, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_Y, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_X_MAX, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_X_MIN, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_Y_MAX, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_Y_MIN, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_Z, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_Z_MIN, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_Z_MAX, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_AS_BINARY, ABinaryTypeComputer.INSTANCE, true);
        addFunction(ST_AS_TEXT, AStringTypeComputer.INSTANCE, true);
        addFunction(ST_AS_GEOJSON, AStringTypeComputer.INSTANCE, true);
        addFunction(ST_DISTANCE, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_LENGTH, ADoubleTypeComputer.INSTANCE, true);
        addFunction(ST_GEOM_FROM_TEXT, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_GEOM_FROM_TEXT_SRID, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_GEOM_FROM_WKB, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_GEOM_FROM_WKB_SRID, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_LINE_FROM_MULTIPOINT, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_MAKE_ENVELOPE, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_IS_CLOSED, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_IS_RING, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_RELATE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(ST_BOUNDARY, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_END_POINT, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_ENVELOPE, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_EXTERIOR_RING, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_GEOMETRY_N, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_INTERIOR_RING_N, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_POINT_N, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_DIFFERENCE, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_START_POINT, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_INTERSECTION, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_SYM_DIFFERENCE, AGeometryTypeComputer.INSTANCE, true);
        addFunction(SCALAR_ST_UNION_AGG, AGeometryTypeComputer.INSTANCE, true);
        addFunction(SCALAR_ST_UNION_AGG_DISTINCT, AGeometryTypeComputer.INSTANCE, true);
        addFunction(SCALAR_ST_UNION_SQL_AGG, AGeometryTypeComputer.INSTANCE, true);
        addFunction(SCALAR_ST_UNION_SQL_AGG_DISTINCT, AGeometryTypeComputer.INSTANCE, true);
        addPrivateFunction(ST_UNION_AGG, AGeometryTypeComputer.INSTANCE, true);
        addPrivateFunction(ST_UNION_SQL_AGG, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_POLYGONIZE, AGeometryTypeComputer.INSTANCE, true);

        addPrivateFunction(ST_MBR, ARectangleTypeComputer.INSTANCE, true);
        addPrivateFunction(ST_MBR_ENLARGE, ARectangleTypeComputer.INSTANCE, true);

        // Binary functions
        addFunction(BINARY_HEX_CONSTRUCTOR, ABinaryTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(BINARY_BASE64_CONSTRUCTOR, ABinaryTypeComputer.INSTANCE_NULLABLE, true);

        addPrivateFunction(SUBSET_COLLECTION, SubsetCollectionTypeComputer.INSTANCE, true);
        addFunction(SWITCH_CASE, SwitchCaseComputer.INSTANCE, true);
        addFunction(SLEEP, SleepTypeComputer.INSTANCE, false);
        addPrivateFunction(INJECT_FAILURE, InjectFailureTypeComputer.INSTANCE, true);
        addPrivateFunction(CAST_TYPE, CastTypeComputer.INSTANCE, true);
        addPrivateFunction(CAST_TYPE_LAX, CastTypeLaxComputer.INSTANCE, true);

        addFunction(TID, AInt64TypeComputer.INSTANCE, true);
        addFunction(TIME_CONSTRUCTOR, ATimeTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(TIME_CONSTRUCTOR_WITH_FORMAT, ATimeTypeComputer.INSTANCE_NULLABLE, true);
        addPrivateFunction(TYPE_OF, AnyTypeComputer.INSTANCE, true);
        addPrivateFunction(UNORDERED_LIST_CONSTRUCTOR, UnorderedListConstructorTypeComputer.INSTANCE, true);
        addFunction(WORD_TOKENS, OrderedListOfAStringTypeComputer.INSTANCE, true);

        // array functions
        addFunction(ARRAY_REMOVE, AListTypeComputer.INSTANCE_REMOVE, true);
        addFunction(ARRAY_PUT, AListTypeComputer.INSTANCE_PUT, true);
        addFunction(ARRAY_PREPEND, AListTypeComputer.INSTANCE_PREPEND, true);
        addFunction(ARRAY_APPEND, AListTypeComputer.INSTANCE_APPEND, true);
        addFunction(ARRAY_INSERT, AListTypeComputer.INSTANCE_INSERT, true);
        addFunction(ARRAY_POSITION, AInt32ArrayPositionTypeComputer.INSTANCE, true);
        addFunction(ARRAY_REPEAT, ArrayRepeatTypeComputer.INSTANCE, true);
        addFunction(ARRAY_REVERSE, AListFirstTypeComputer.INSTANCE, true);
        addFunction(ARRAY_CONTAINS, ABooleanArrayContainsTypeComputer.INSTANCE, true);
        addFunction(ARRAY_SORT, AListFirstTypeComputer.INSTANCE, true);
        addFunction(ARRAY_DISTINCT, AListFirstTypeComputer.INSTANCE, true);
        addFunction(ARRAY_UNION, AListMultiListArgsTypeComputer.INSTANCE, true);
        addFunction(ARRAY_INTERSECT, AListMultiListArgsTypeComputer.INSTANCE, true);
        addFunction(ARRAY_IFNULL, ArrayIfNullTypeComputer.INSTANCE, true);
        addFunction(ARRAY_CONCAT, AListMultiListArgsTypeComputer.INSTANCE, true);
        addFunction(ARRAY_RANGE_WITH_STEP, ArrayRangeTypeComputer.INSTANCE, true);
        addFunction(ARRAY_RANGE_WITHOUT_STEP, ArrayRangeTypeComputer.INSTANCE, true);
        addFunction(ARRAY_FLATTEN, AListFirstTypeComputer.INSTANCE_FLATTEN, true);
        addFunction(ARRAY_REPLACE_WITH_MAXIMUM, AListTypeComputer.INSTANCE_REPLACE, true);
        addFunction(ARRAY_REPLACE_WITHOUT_MAXIMUM, AListTypeComputer.INSTANCE_REPLACE, true);
        addFunction(ARRAY_SYMDIFF, AListMultiListArgsTypeComputer.INSTANCE, true);
        addFunction(ARRAY_SYMDIFFN, AListMultiListArgsTypeComputer.INSTANCE, true);
        addFunction(ARRAY_STAR, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(ARRAY_SLICE_WITH_END_POSITION, AListTypeComputer.INSTANCE_SLICE, true);
        addFunction(ARRAY_SLICE_WITHOUT_END_POSITION, AListTypeComputer.INSTANCE_SLICE, true);
        addFunction(ARRAY_EXCEPT, ArrayExceptTypeComputer.INSTANCE, true);
        addFunction(ARRAY_MOVE, AListTypeComputer.INSTANCE_MOVE, true);
        addFunction(ARRAY_SWAP, AListTypeComputer.INSTANCE_SWAP, true);
        addFunction(ARRAY_BINARY_SEARCH, AInt32TypeComputer.INSTANCE_NULLABLE, true);

        // objects
        addFunction(RECORD_MERGE, RecordMergeTypeComputer.INSTANCE, true);
        addPrivateFunction(RECORD_MERGE_IGNORE_DUPLICATES, RecordMergeTypeComputer.INSTANCE_IGNORE_DUPLICATES, true);
        addFunction(RECORD_CONCAT, OpenARecordTypeComputer.INSTANCE, true);
        addPrivateFunction(RECORD_CONCAT_STRICT, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(ADD_FIELDS, RecordAddFieldsTypeComputer.INSTANCE, true);
        addFunction(REMOVE_FIELDS, RecordRemoveFieldsTypeComputer.INSTANCE, true);
        addPrivateFunction(CLOSED_RECORD_CONSTRUCTOR, ClosedRecordConstructorResultType.INSTANCE, true);
        addPrivateFunction(OPEN_RECORD_CONSTRUCTOR, OpenRecordConstructorResultType.INSTANCE, true);
        addPrivateFunction(FIELD_ACCESS_BY_INDEX, FieldAccessByIndexResultType.INSTANCE, true);
        addPrivateFunction(FIELD_ACCESS_NESTED, FieldAccessNestedResultType.INSTANCE, true);
        addFunction(FIELD_ACCESS_BY_NAME, FieldAccessByNameResultType.INSTANCE, true);
        addFunction(GET_RECORD_FIELDS, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addFunction(GET_RECORD_FIELD_VALUE, FieldAccessNestedResultType.INSTANCE, true);
        addFunction(RECORD_LENGTH, AInt64TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(RECORD_NAMES, OrderedListOfAStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(RECORD_PAIRS, OrderedListOfAnyTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(PAIRS, OrderedListOfAnyTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(GEOMETRY_CONSTRUCTOR, AGeometryTypeComputer.INSTANCE, true);
        addFunction(RECORD_REMOVE, RecordRemoveTypeComputer.INSTANCE, true);
        addFunction(RECORD_RENAME, RecordRenameTypeComputer.INSTANCE, true);
        addFunction(RECORD_UNWRAP, AnyTypeComputer.INSTANCE, true);
        addFunction(RECORD_REPLACE, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(RECORD_ADD, RecordAddTypeComputer.INSTANCE, true);
        addFunction(RECORD_PUT, RecordPutTypeComputer.INSTANCE, true);
        addFunction(RECORD_VALUES, OrderedListOfAnyTypeComputer.INSTANCE, true);

        // temporal type accessors
        addFunction(ACCESSOR_TEMPORAL_YEAR, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_MONTH, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_DAY, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_HOUR, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_MIN, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_SEC, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_MILLISEC, AInt64TypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_START, ATemporalInstanceTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_END, ATemporalInstanceTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_START_DATETIME, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_END_DATETIME, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_START_DATE, ADateTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_END_DATE, ADateTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_START_TIME, ATimeTypeComputer.INSTANCE, true);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_END_TIME, ATimeTypeComputer.INSTANCE, true);

        // temporal functions
        addFunction(UNIX_TIME_FROM_DATE_IN_DAYS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_DATE_IN_MS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_TIME_IN_MS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_DATETIME_IN_MS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_DATETIME_IN_MS_WITH_TZ, AInt64TypeComputer.INSTANCE, false);
        addFunction(UNIX_TIME_FROM_DATETIME_IN_SECS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_DATETIME_IN_SECS_WITH_TZ, AInt64TypeComputer.INSTANCE, false);
        addFunction(DATE_FROM_UNIX_TIME_IN_DAYS, ADateTypeComputer.INSTANCE, true);
        addFunction(DATE_FROM_DATETIME, ADateTypeComputer.INSTANCE, true);
        addFunction(TIME_FROM_UNIX_TIME_IN_MS, ATimeTypeComputer.INSTANCE, true);
        addFunction(TIME_FROM_DATETIME, ATimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_DATE_TIME, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_MS, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_MS_WITH_TZ, ADateTimeTypeComputer.INSTANCE, false);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_SECS, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_SECS_WITH_TZ, ADateTimeTypeComputer.INSTANCE, false);
        addFunction(CALENDAR_DURATION_FROM_DATETIME, ADurationTypeComputer.INSTANCE, true);
        addFunction(CALENDAR_DURATION_FROM_DATE, ADurationTypeComputer.INSTANCE, true);
        addFunction(ADJUST_DATETIME_FOR_TIMEZONE, AStringTypeComputer.INSTANCE, true);
        addFunction(ADJUST_TIME_FOR_TIMEZONE, AStringTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_BEFORE, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_AFTER, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_MEETS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_MET_BY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_OVERLAPS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_OVERLAPPED_BY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_OVERLAPPING, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_STARTS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_STARTED_BY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_COVERS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_COVERED_BY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_ENDS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_ENDED_BY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(CURRENT_DATE, ADateTypeComputer.INSTANCE, false);
        addFunction(CURRENT_DATE_IMMEDIATE, ADateTypeComputer.INSTANCE, false);
        addFunction(CURRENT_TIME, ATimeTypeComputer.INSTANCE, false);
        addFunction(CURRENT_TIME_IMMEDIATE, ATimeTypeComputer.INSTANCE, false);
        addFunction(CURRENT_DATETIME, ADateTimeTypeComputer.INSTANCE, false);
        addFunction(CURRENT_DATETIME_IMMEDIATE, ADateTimeTypeComputer.INSTANCE, false);
        addPrivateFunction(DAY_TIME_DURATION_GREATER_THAN, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(DAY_TIME_DURATION_LESS_THAN, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(YEAR_MONTH_DURATION_GREATER_THAN, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(YEAR_MONTH_DURATION_LESS_THAN, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(DURATION_EQUAL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(DURATION_FROM_MONTHS, ADurationTypeComputer.INSTANCE, true);
        addFunction(DURATION_FROM_MILLISECONDS, ADurationTypeComputer.INSTANCE, true);
        addFunction(MONTHS_FROM_YEAR_MONTH_DURATION, AInt64TypeComputer.INSTANCE, true);
        addFunction(MILLISECONDS_FROM_DAY_TIME_DURATION, AInt64TypeComputer.INSTANCE, true);
        addFunction(GET_DAY_TIME_DURATION, ADayTimeDurationTypeComputer.INSTANCE, true);
        addFunction(GET_YEAR_MONTH_DURATION, AYearMonthDurationTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_BIN, AIntervalTypeComputer.INSTANCE, true);
        addFunction(DAY_OF_WEEK, AInt64TypeComputer.INSTANCE, true);
        addFunction(DAY_OF_WEEK2, AInt64TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(DAY_OF_YEAR, AInt64TypeComputer.INSTANCE, true);
        addFunction(QUARTER_OF_YEAR, AInt64TypeComputer.INSTANCE, true);
        addFunction(WEEK_OF_YEAR, AInt64TypeComputer.INSTANCE, true);
        addFunction(WEEK_OF_YEAR2, AInt64TypeComputer.INSTANCE_NULLABLE, true);
        addFunction(PARSE_DATE, ADateTypeComputer.INSTANCE, true);
        addFunction(PARSE_TIME, ATimeTypeComputer.INSTANCE, true);
        addFunction(PARSE_DATETIME, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(PRINT_DATE, AStringTypeComputer.INSTANCE, true);
        addFunction(PRINT_TIME, AStringTypeComputer.INSTANCE, true);
        addFunction(PRINT_DATETIME, AStringTypeComputer.INSTANCE, true);
        addFunction(OVERLAP_BINS, OrderedListOfAIntervalTypeComputer.INSTANCE, true);
        addFunction(GET_OVERLAPPING_INTERVAL, GetOverlappingInvervalTypeComputer.INSTANCE, true);
        addFunction(DURATION_FROM_INTERVAL, ADayTimeDurationTypeComputer.INSTANCE, true);

        // interval constructors
        addFunction(INTERVAL_CONSTRUCTOR, AIntervalTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_CONSTRUCTOR_START_FROM_DATE, AIntervalTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_CONSTRUCTOR_START_FROM_DATETIME, AIntervalTypeComputer.INSTANCE, true);
        addFunction(INTERVAL_CONSTRUCTOR_START_FROM_TIME, AIntervalTypeComputer.INSTANCE, true);

        // meta() function
        addFunction(META, OpenARecordTypeComputer.INSTANCE, true);
        addPrivateFunction(META_KEY, AnyTypeComputer.INSTANCE, false);

        addFunction(DECODE_DATAVERSE_NAME, OrderedListOfAStringTypeComputer.INSTANCE_NULLABLE, true);

        addPrivateFunction(COLLECTION_TO_SEQUENCE, CollectionToSequenceTypeComputer.INSTANCE, true);
        addFunction(SERIALIZED_SIZE, AInt64TypeComputer.INSTANCE, true);
        // used by CBO's internal sampling queries for determining projection sizes

        // external lookup
        addPrivateFunction(EXTERNAL_LOOKUP, AnyTypeComputer.INSTANCE, false);

        // get job parameter
        addFunction(GET_JOB_PARAMETER, AnyTypeComputer.INSTANCE, false);

        // unnesting function
        addPrivateFunction(SCAN_COLLECTION, CollectionMemberResultType.INSTANCE, true);

    }

    static {
        //  Aggregate functions

        // AVG

        addAgg(AVG);
        addAgg(LOCAL_AVG);
        addAgg(GLOBAL_AVG);
        addLocalAgg(AVG, LOCAL_AVG);
        addIntermediateAgg(AVG, INTERMEDIATE_AVG);
        addIntermediateAgg(LOCAL_AVG, INTERMEDIATE_AVG);
        addIntermediateAgg(GLOBAL_AVG, INTERMEDIATE_AVG);
        addGlobalAgg(AVG, GLOBAL_AVG);

        addScalarAgg(AVG, SCALAR_AVG);

        addSerialAgg(AVG, SERIAL_AVG);
        addSerialAgg(LOCAL_AVG, SERIAL_LOCAL_AVG);
        addSerialAgg(GLOBAL_AVG, SERIAL_GLOBAL_AVG);
        addAgg(SERIAL_AVG);
        addAgg(SERIAL_LOCAL_AVG);
        addAgg(SERIAL_GLOBAL_AVG);
        addLocalAgg(SERIAL_AVG, SERIAL_LOCAL_AVG);
        addIntermediateAgg(SERIAL_AVG, SERIAL_INTERMEDIATE_AVG);
        addIntermediateAgg(SERIAL_LOCAL_AVG, SERIAL_INTERMEDIATE_AVG);
        addIntermediateAgg(SERIAL_GLOBAL_AVG, SERIAL_INTERMEDIATE_AVG);
        addGlobalAgg(SERIAL_AVG, SERIAL_GLOBAL_AVG);

        // AVG DISTINCT

        addDistinctAgg(AVG_DISTINCT, AVG);
        addScalarAgg(AVG_DISTINCT, SCALAR_AVG_DISTINCT);

        // COUNT

        addAgg(COUNT);
        addLocalAgg(COUNT, COUNT);
        addIntermediateAgg(COUNT, SUM);
        addGlobalAgg(COUNT, SUM);

        addScalarAgg(COUNT, SCALAR_COUNT);

        addSerialAgg(COUNT, SERIAL_COUNT);
        addAgg(SERIAL_COUNT);
        addLocalAgg(SERIAL_COUNT, SERIAL_COUNT);
        addIntermediateAgg(SERIAL_COUNT, SERIAL_SUM);
        addGlobalAgg(SERIAL_COUNT, SERIAL_SUM);

        // COUNT DISTINCT

        addDistinctAgg(COUNT_DISTINCT, COUNT);
        addScalarAgg(COUNT_DISTINCT, SCALAR_COUNT_DISTINCT);

        // MAX
        addAgg(MAX);
        addAgg(LOCAL_MAX);
        addAgg(GLOBAL_MAX);
        addLocalAgg(MAX, LOCAL_MAX);
        addIntermediateAgg(LOCAL_MAX, INTERMEDIATE_MAX);
        addIntermediateAgg(GLOBAL_MAX, GLOBAL_MAX);
        addIntermediateAgg(MAX, GLOBAL_MAX);
        addGlobalAgg(MAX, GLOBAL_MAX);

        addScalarAgg(MAX, SCALAR_MAX);

        // MAX DISTINCT
        addDistinctAgg(MAX_DISTINCT, MAX);
        addScalarAgg(MAX_DISTINCT, SCALAR_MAX_DISTINCT);

        // STDDEV_SAMP

        addAgg(STDDEV_SAMP);
        addAgg(LOCAL_STDDEV_SAMP);
        addAgg(GLOBAL_STDDEV_SAMP);
        addLocalAgg(STDDEV_SAMP, LOCAL_STDDEV_SAMP);
        addIntermediateAgg(STDDEV_SAMP, INTERMEDIATE_STDDEV_SAMP);
        addIntermediateAgg(LOCAL_STDDEV_SAMP, INTERMEDIATE_STDDEV_SAMP);
        addIntermediateAgg(GLOBAL_STDDEV_SAMP, INTERMEDIATE_STDDEV_SAMP);
        addGlobalAgg(STDDEV_SAMP, GLOBAL_STDDEV_SAMP);

        addScalarAgg(STDDEV_SAMP, SCALAR_STDDEV_SAMP);

        addSerialAgg(STDDEV_SAMP, SERIAL_STDDEV_SAMP);
        addSerialAgg(LOCAL_STDDEV_SAMP, SERIAL_LOCAL_STDDEV_SAMP);
        addSerialAgg(GLOBAL_STDDEV_SAMP, SERIAL_GLOBAL_STDDEV_SAMP);
        addAgg(SERIAL_STDDEV_SAMP);
        addAgg(SERIAL_LOCAL_STDDEV_SAMP);
        addAgg(SERIAL_GLOBAL_STDDEV_SAMP);
        addLocalAgg(SERIAL_STDDEV_SAMP, SERIAL_LOCAL_STDDEV_SAMP);
        addIntermediateAgg(SERIAL_STDDEV_SAMP, SERIAL_INTERMEDIATE_STDDEV_SAMP);
        addIntermediateAgg(SERIAL_LOCAL_STDDEV_SAMP, SERIAL_INTERMEDIATE_STDDEV_SAMP);
        addIntermediateAgg(SERIAL_GLOBAL_STDDEV_SAMP, SERIAL_INTERMEDIATE_STDDEV_SAMP);
        addGlobalAgg(SERIAL_STDDEV_SAMP, SERIAL_GLOBAL_STDDEV_SAMP);

        // STDDEV_SAMP DISTINCT

        addDistinctAgg(STDDEV_SAMP_DISTINCT, STDDEV_SAMP);
        addScalarAgg(STDDEV_SAMP_DISTINCT, SCALAR_STDDEV_SAMP_DISTINCT);

        // STDDEV_POP

        addAgg(STDDEV_POP);
        addAgg(LOCAL_STDDEV_POP);
        addAgg(GLOBAL_STDDEV_POP);
        addLocalAgg(STDDEV_POP, LOCAL_STDDEV_POP);
        addIntermediateAgg(STDDEV_POP, INTERMEDIATE_STDDEV_POP);
        addIntermediateAgg(LOCAL_STDDEV_POP, INTERMEDIATE_STDDEV_POP);
        addIntermediateAgg(GLOBAL_STDDEV_POP, INTERMEDIATE_STDDEV_POP);
        addGlobalAgg(STDDEV_POP, GLOBAL_STDDEV_POP);

        addScalarAgg(STDDEV_POP, SCALAR_STDDEV_POP);

        addSerialAgg(STDDEV_POP, SERIAL_STDDEV_POP);
        addSerialAgg(LOCAL_STDDEV_POP, SERIAL_LOCAL_STDDEV_POP);
        addSerialAgg(GLOBAL_STDDEV_POP, SERIAL_GLOBAL_STDDEV_POP);
        addAgg(SERIAL_STDDEV_POP);
        addAgg(SERIAL_LOCAL_STDDEV_POP);
        addAgg(SERIAL_GLOBAL_STDDEV_POP);
        addLocalAgg(SERIAL_STDDEV_POP, SERIAL_LOCAL_STDDEV_POP);
        addIntermediateAgg(SERIAL_STDDEV_POP, SERIAL_INTERMEDIATE_STDDEV_POP);
        addIntermediateAgg(SERIAL_LOCAL_STDDEV_POP, SERIAL_INTERMEDIATE_STDDEV_POP);
        addIntermediateAgg(SERIAL_GLOBAL_STDDEV_POP, SERIAL_INTERMEDIATE_STDDEV_POP);
        addGlobalAgg(SERIAL_STDDEV_POP, SERIAL_GLOBAL_STDDEV_POP);

        // STDDEV_POP DISTINCT

        addDistinctAgg(STDDEV_POP_DISTINCT, STDDEV_POP);
        addScalarAgg(STDDEV_POP_DISTINCT, SCALAR_STDDEV_POP_DISTINCT);

        // VAR_SAMP

        addAgg(VAR_SAMP);
        addAgg(LOCAL_VAR_SAMP);
        addAgg(GLOBAL_VAR_SAMP);
        addLocalAgg(VAR_SAMP, LOCAL_VAR_SAMP);
        addIntermediateAgg(VAR_SAMP, INTERMEDIATE_VAR_SAMP);
        addIntermediateAgg(LOCAL_VAR_SAMP, INTERMEDIATE_VAR_SAMP);
        addIntermediateAgg(GLOBAL_VAR_SAMP, INTERMEDIATE_VAR_SAMP);
        addGlobalAgg(VAR_SAMP, GLOBAL_VAR_SAMP);

        addScalarAgg(VAR_SAMP, SCALAR_VAR_SAMP);

        addSerialAgg(VAR_SAMP, SERIAL_VAR_SAMP);
        addSerialAgg(LOCAL_VAR_SAMP, SERIAL_LOCAL_VAR_SAMP);
        addSerialAgg(GLOBAL_VAR_SAMP, SERIAL_GLOBAL_VAR_SAMP);
        addAgg(SERIAL_VAR_SAMP);
        addAgg(SERIAL_LOCAL_VAR_SAMP);
        addAgg(SERIAL_GLOBAL_VAR_SAMP);
        addLocalAgg(SERIAL_VAR_SAMP, SERIAL_LOCAL_VAR_SAMP);
        addIntermediateAgg(SERIAL_VAR_SAMP, SERIAL_INTERMEDIATE_VAR_SAMP);
        addIntermediateAgg(SERIAL_LOCAL_VAR_SAMP, SERIAL_INTERMEDIATE_VAR_SAMP);
        addIntermediateAgg(SERIAL_GLOBAL_VAR_SAMP, SERIAL_INTERMEDIATE_VAR_SAMP);
        addGlobalAgg(SERIAL_VAR_SAMP, SERIAL_GLOBAL_VAR_SAMP);

        // VAR_SAMP DISTINCT

        addDistinctAgg(VAR_SAMP_DISTINCT, VAR_SAMP);
        addScalarAgg(VAR_SAMP_DISTINCT, SCALAR_VAR_SAMP_DISTINCT);

        // VAR_POP

        addAgg(VAR_POP);
        addAgg(LOCAL_VAR_POP);
        addAgg(GLOBAL_VAR_POP);
        addLocalAgg(VAR_POP, LOCAL_VAR_POP);
        addIntermediateAgg(VAR_POP, INTERMEDIATE_VAR_POP);
        addIntermediateAgg(LOCAL_VAR_POP, INTERMEDIATE_VAR_POP);
        addIntermediateAgg(GLOBAL_VAR_POP, INTERMEDIATE_VAR_POP);
        addGlobalAgg(VAR_POP, GLOBAL_VAR_POP);

        addScalarAgg(VAR_POP, SCALAR_VAR_POP);

        addSerialAgg(VAR_POP, SERIAL_VAR_POP);
        addSerialAgg(LOCAL_VAR_POP, SERIAL_LOCAL_VAR_POP);
        addSerialAgg(GLOBAL_VAR_POP, SERIAL_GLOBAL_VAR_POP);
        addAgg(SERIAL_VAR_POP);
        addAgg(SERIAL_LOCAL_VAR_POP);
        addAgg(SERIAL_GLOBAL_VAR_POP);
        addLocalAgg(SERIAL_VAR_POP, SERIAL_LOCAL_VAR_POP);
        addIntermediateAgg(SERIAL_VAR_POP, SERIAL_INTERMEDIATE_VAR_POP);
        addIntermediateAgg(SERIAL_LOCAL_VAR_POP, SERIAL_INTERMEDIATE_VAR_POP);
        addIntermediateAgg(SERIAL_GLOBAL_VAR_POP, SERIAL_INTERMEDIATE_VAR_POP);
        addGlobalAgg(SERIAL_VAR_POP, SERIAL_GLOBAL_VAR_POP);

        // VAR_POP DISTINCT

        addDistinctAgg(VAR_POP_DISTINCT, VAR_POP);
        addScalarAgg(VAR_POP_DISTINCT, SCALAR_VAR_POP_DISTINCT);

        // SKEWNESS

        addAgg(SKEWNESS);
        addAgg(LOCAL_SKEWNESS);
        addAgg(GLOBAL_SKEWNESS);
        addLocalAgg(SKEWNESS, LOCAL_SKEWNESS);
        addIntermediateAgg(SKEWNESS, INTERMEDIATE_SKEWNESS);
        addIntermediateAgg(LOCAL_SKEWNESS, INTERMEDIATE_SKEWNESS);
        addIntermediateAgg(GLOBAL_SKEWNESS, INTERMEDIATE_SKEWNESS);
        addGlobalAgg(SKEWNESS, GLOBAL_SKEWNESS);

        addScalarAgg(SKEWNESS, SCALAR_SKEWNESS);

        addSerialAgg(SKEWNESS, SERIAL_SKEWNESS);
        addSerialAgg(LOCAL_SKEWNESS, SERIAL_LOCAL_SKEWNESS);
        addSerialAgg(GLOBAL_SKEWNESS, SERIAL_GLOBAL_SKEWNESS);
        addAgg(SERIAL_SKEWNESS);
        addAgg(SERIAL_LOCAL_SKEWNESS);
        addAgg(SERIAL_GLOBAL_SKEWNESS);
        addLocalAgg(SERIAL_SKEWNESS, SERIAL_LOCAL_SKEWNESS);
        addIntermediateAgg(SERIAL_SKEWNESS, SERIAL_INTERMEDIATE_SKEWNESS);
        addIntermediateAgg(SERIAL_LOCAL_SKEWNESS, SERIAL_INTERMEDIATE_SKEWNESS);
        addIntermediateAgg(SERIAL_GLOBAL_SKEWNESS, SERIAL_INTERMEDIATE_SKEWNESS);
        addGlobalAgg(SERIAL_SKEWNESS, SERIAL_GLOBAL_SKEWNESS);

        // SKEWNESS DISTINCT

        addDistinctAgg(SKEWNESS_DISTINCT, SKEWNESS);
        addScalarAgg(SKEWNESS_DISTINCT, SCALAR_SKEWNESS_DISTINCT);

        // KURTOSIS

        addAgg(KURTOSIS);
        addAgg(LOCAL_KURTOSIS);
        addAgg(GLOBAL_KURTOSIS);
        addLocalAgg(KURTOSIS, LOCAL_KURTOSIS);
        addIntermediateAgg(KURTOSIS, INTERMEDIATE_KURTOSIS);
        addIntermediateAgg(LOCAL_KURTOSIS, INTERMEDIATE_KURTOSIS);
        addIntermediateAgg(GLOBAL_KURTOSIS, INTERMEDIATE_KURTOSIS);
        addGlobalAgg(KURTOSIS, GLOBAL_KURTOSIS);

        addScalarAgg(KURTOSIS, SCALAR_KURTOSIS);

        addSerialAgg(KURTOSIS, SERIAL_KURTOSIS);
        addSerialAgg(LOCAL_KURTOSIS, SERIAL_LOCAL_KURTOSIS);
        addSerialAgg(GLOBAL_KURTOSIS, SERIAL_GLOBAL_KURTOSIS);
        addAgg(SERIAL_KURTOSIS);
        addAgg(SERIAL_LOCAL_KURTOSIS);
        addAgg(SERIAL_GLOBAL_KURTOSIS);
        addLocalAgg(SERIAL_KURTOSIS, SERIAL_LOCAL_KURTOSIS);
        addIntermediateAgg(SERIAL_KURTOSIS, SERIAL_INTERMEDIATE_KURTOSIS);
        addIntermediateAgg(SERIAL_LOCAL_KURTOSIS, SERIAL_INTERMEDIATE_KURTOSIS);
        addIntermediateAgg(SERIAL_GLOBAL_KURTOSIS, SERIAL_INTERMEDIATE_KURTOSIS);
        addGlobalAgg(SERIAL_KURTOSIS, SERIAL_GLOBAL_KURTOSIS);

        // KURTOSIS DISTINCT

        addDistinctAgg(KURTOSIS_DISTINCT, KURTOSIS);
        addScalarAgg(KURTOSIS_DISTINCT, SCALAR_KURTOSIS_DISTINCT);

        // FIRST_ELEMENT

        addAgg(FIRST_ELEMENT);
        addAgg(LOCAL_FIRST_ELEMENT);
        addLocalAgg(FIRST_ELEMENT, LOCAL_FIRST_ELEMENT);
        addIntermediateAgg(LOCAL_FIRST_ELEMENT, FIRST_ELEMENT);
        addIntermediateAgg(FIRST_ELEMENT, FIRST_ELEMENT);
        addGlobalAgg(FIRST_ELEMENT, FIRST_ELEMENT);

        addScalarAgg(FIRST_ELEMENT, SCALAR_FIRST_ELEMENT);
        addScalarAgg(LOCAL_FIRST_ELEMENT, SCALAR_LOCAL_FIRST_ELEMENT);

        // LAST_ELEMENT

        addAgg(LAST_ELEMENT);
        addScalarAgg(LAST_ELEMENT, SCALAR_LAST_ELEMENT);

        // RANGE_MAP
        addAgg(RANGE_MAP);
        addAgg(LOCAL_SAMPLING);
        addLocalAgg(RANGE_MAP, LOCAL_SAMPLING);
        addIntermediateAgg(LOCAL_SAMPLING, RANGE_MAP);
        addIntermediateAgg(RANGE_MAP, RANGE_MAP);
        addGlobalAgg(RANGE_MAP, RANGE_MAP);

        addAgg(NULL_WRITER);
        addLocalAgg(NULL_WRITER, NULL_WRITER);
        addIntermediateAgg(NULL_WRITER, NULL_WRITER);
        addGlobalAgg(NULL_WRITER, NULL_WRITER);

        // MIN
        addAgg(MIN);
        addAgg(LOCAL_MIN);
        addAgg(GLOBAL_MIN);
        addLocalAgg(MIN, LOCAL_MIN);
        addIntermediateAgg(LOCAL_MIN, INTERMEDIATE_MIN);
        addIntermediateAgg(GLOBAL_MIN, GLOBAL_MIN);
        addIntermediateAgg(MIN, GLOBAL_MIN);
        addGlobalAgg(MIN, GLOBAL_MIN);

        addScalarAgg(MIN, SCALAR_MIN);

        // MIN DISTINCT
        addDistinctAgg(MIN_DISTINCT, MIN);
        addScalarAgg(MIN_DISTINCT, SCALAR_MIN_DISTINCT);

        // SUM
        addAgg(SUM);
        addAgg(LOCAL_SUM);
        addAgg(GLOBAL_SUM);
        addLocalAgg(SUM, LOCAL_SUM);
        addIntermediateAgg(SUM, INTERMEDIATE_SUM);
        addIntermediateAgg(LOCAL_SUM, INTERMEDIATE_SUM);
        addIntermediateAgg(GLOBAL_SUM, INTERMEDIATE_SUM);
        addGlobalAgg(SUM, GLOBAL_SUM);
        addScalarAgg(SUM, SCALAR_SUM);

        addAgg(SERIAL_SUM);
        addAgg(SERIAL_LOCAL_SUM);
        addAgg(SERIAL_GLOBAL_SUM);
        addSerialAgg(SUM, SERIAL_SUM);
        addSerialAgg(LOCAL_SUM, SERIAL_LOCAL_SUM);
        addSerialAgg(GLOBAL_SUM, SERIAL_GLOBAL_SUM);
        addLocalAgg(SERIAL_SUM, SERIAL_LOCAL_SUM);
        addIntermediateAgg(SERIAL_SUM, SERIAL_INTERMEDIATE_SUM);
        addIntermediateAgg(SERIAL_LOCAL_SUM, SERIAL_INTERMEDIATE_SUM);
        addIntermediateAgg(SERIAL_GLOBAL_SUM, SERIAL_INTERMEDIATE_SUM);
        addGlobalAgg(SERIAL_SUM, SERIAL_GLOBAL_SUM);

        // SUM DISTINCT
        addDistinctAgg(SUM_DISTINCT, SUM);
        addScalarAgg(SUM_DISTINCT, SCALAR_SUM_DISTINCT);

        // LISTIFY/ARRAY_AGG

        addAgg(LISTIFY);
        addScalarAgg(LISTIFY, SCALAR_ARRAYAGG);

        // LISTIFY/ARRAY_AGG DISTINCT

        addDistinctAgg(LISTIFY_DISTINCT, LISTIFY);
        addScalarAgg(LISTIFY_DISTINCT, SCALAR_ARRAYAGG_DISTINCT);

        // SQL Aggregate Functions

        // SQL AVG

        addAgg(SQL_AVG);
        addAgg(LOCAL_SQL_AVG);
        addAgg(GLOBAL_SQL_AVG);
        addLocalAgg(SQL_AVG, LOCAL_SQL_AVG);
        addIntermediateAgg(SQL_AVG, INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(LOCAL_SQL_AVG, INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(GLOBAL_SQL_AVG, INTERMEDIATE_SQL_AVG);
        addGlobalAgg(SQL_AVG, GLOBAL_SQL_AVG);

        addScalarAgg(SQL_AVG, SCALAR_SQL_AVG);

        addSerialAgg(SQL_AVG, SERIAL_SQL_AVG);
        addSerialAgg(LOCAL_SQL_AVG, SERIAL_LOCAL_SQL_AVG);
        addSerialAgg(GLOBAL_SQL_AVG, SERIAL_GLOBAL_SQL_AVG);
        addAgg(SERIAL_SQL_AVG);
        addAgg(SERIAL_LOCAL_SQL_AVG);
        addAgg(SERIAL_GLOBAL_SQL_AVG);
        addLocalAgg(SERIAL_SQL_AVG, SERIAL_LOCAL_SQL_AVG);
        addIntermediateAgg(SERIAL_SQL_AVG, SERIAL_INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(SERIAL_LOCAL_SQL_AVG, SERIAL_INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_AVG, SERIAL_INTERMEDIATE_SQL_AVG);
        addGlobalAgg(SERIAL_SQL_AVG, SERIAL_GLOBAL_SQL_AVG);

        // SQL STDDEV_SAMP

        addAgg(SQL_STDDEV_SAMP);
        addAgg(LOCAL_SQL_STDDEV_SAMP);
        addAgg(GLOBAL_SQL_STDDEV_SAMP);
        addLocalAgg(SQL_STDDEV_SAMP, LOCAL_SQL_STDDEV_SAMP);
        addIntermediateAgg(SQL_STDDEV_SAMP, INTERMEDIATE_SQL_STDDEV_SAMP);
        addIntermediateAgg(LOCAL_SQL_STDDEV_SAMP, INTERMEDIATE_SQL_STDDEV_SAMP);
        addIntermediateAgg(GLOBAL_SQL_STDDEV_SAMP, INTERMEDIATE_SQL_STDDEV_SAMP);
        addGlobalAgg(SQL_STDDEV_SAMP, GLOBAL_SQL_STDDEV_SAMP);

        addScalarAgg(SQL_STDDEV_SAMP, SCALAR_SQL_STDDEV_SAMP);

        addSerialAgg(SQL_STDDEV_SAMP, SERIAL_SQL_STDDEV_SAMP);
        addSerialAgg(LOCAL_SQL_STDDEV_SAMP, SERIAL_LOCAL_SQL_STDDEV_SAMP);
        addSerialAgg(GLOBAL_SQL_STDDEV_SAMP, SERIAL_GLOBAL_SQL_STDDEV_SAMP);
        addAgg(SERIAL_SQL_STDDEV_SAMP);
        addAgg(SERIAL_LOCAL_SQL_STDDEV_SAMP);
        addAgg(SERIAL_GLOBAL_SQL_STDDEV_SAMP);
        addLocalAgg(SERIAL_SQL_STDDEV_SAMP, SERIAL_LOCAL_SQL_STDDEV_SAMP);
        addIntermediateAgg(SERIAL_SQL_STDDEV_SAMP, SERIAL_INTERMEDIATE_SQL_STDDEV_SAMP);
        addIntermediateAgg(SERIAL_LOCAL_SQL_STDDEV_SAMP, SERIAL_INTERMEDIATE_SQL_STDDEV_SAMP);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_STDDEV_SAMP, SERIAL_INTERMEDIATE_SQL_STDDEV_SAMP);
        addGlobalAgg(SERIAL_SQL_STDDEV_SAMP, SERIAL_GLOBAL_SQL_STDDEV_SAMP);

        // SQL STDDEV_POP

        addAgg(SQL_STDDEV_POP);
        addAgg(LOCAL_SQL_STDDEV_POP);
        addAgg(GLOBAL_SQL_STDDEV_POP);
        addLocalAgg(SQL_STDDEV_POP, LOCAL_SQL_STDDEV_POP);
        addIntermediateAgg(SQL_STDDEV_POP, INTERMEDIATE_SQL_STDDEV_POP);
        addIntermediateAgg(LOCAL_SQL_STDDEV_POP, INTERMEDIATE_SQL_STDDEV_POP);
        addIntermediateAgg(GLOBAL_SQL_STDDEV_POP, INTERMEDIATE_SQL_STDDEV_POP);
        addGlobalAgg(SQL_STDDEV_POP, GLOBAL_SQL_STDDEV_POP);

        addScalarAgg(SQL_STDDEV_POP, SCALAR_SQL_STDDEV_POP);

        addSerialAgg(SQL_STDDEV_POP, SERIAL_SQL_STDDEV_POP);
        addSerialAgg(LOCAL_SQL_STDDEV_POP, SERIAL_LOCAL_SQL_STDDEV_POP);
        addSerialAgg(GLOBAL_SQL_STDDEV_POP, SERIAL_GLOBAL_SQL_STDDEV_POP);
        addAgg(SERIAL_SQL_STDDEV_POP);
        addAgg(SERIAL_LOCAL_SQL_STDDEV_POP);
        addAgg(SERIAL_GLOBAL_SQL_STDDEV_POP);
        addLocalAgg(SERIAL_SQL_STDDEV_POP, SERIAL_LOCAL_SQL_STDDEV_POP);
        addIntermediateAgg(SERIAL_SQL_STDDEV_POP, SERIAL_INTERMEDIATE_SQL_STDDEV_POP);
        addIntermediateAgg(SERIAL_LOCAL_SQL_STDDEV_POP, SERIAL_INTERMEDIATE_SQL_STDDEV_POP);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_STDDEV_POP, SERIAL_INTERMEDIATE_SQL_STDDEV_POP);
        addGlobalAgg(SERIAL_SQL_STDDEV_POP, SERIAL_GLOBAL_SQL_STDDEV_POP);

        // SQL VAR_SAMP

        addAgg(SQL_VAR_SAMP);
        addAgg(LOCAL_SQL_VAR_SAMP);
        addAgg(GLOBAL_SQL_VAR_SAMP);
        addLocalAgg(SQL_VAR_SAMP, LOCAL_SQL_VAR_SAMP);
        addIntermediateAgg(SQL_VAR_SAMP, INTERMEDIATE_SQL_VAR_SAMP);
        addIntermediateAgg(LOCAL_SQL_VAR_SAMP, INTERMEDIATE_SQL_VAR_SAMP);
        addIntermediateAgg(GLOBAL_SQL_VAR_SAMP, INTERMEDIATE_SQL_VAR_SAMP);
        addGlobalAgg(SQL_VAR_SAMP, GLOBAL_SQL_VAR_SAMP);

        addScalarAgg(SQL_VAR_SAMP, SCALAR_SQL_VAR_SAMP);

        addSerialAgg(SQL_VAR_SAMP, SERIAL_SQL_VAR_SAMP);
        addSerialAgg(LOCAL_SQL_VAR_SAMP, SERIAL_LOCAL_SQL_VAR_SAMP);
        addSerialAgg(GLOBAL_SQL_VAR_SAMP, SERIAL_GLOBAL_SQL_VAR_SAMP);
        addAgg(SERIAL_SQL_VAR_SAMP);
        addAgg(SERIAL_LOCAL_SQL_VAR_SAMP);
        addAgg(SERIAL_GLOBAL_SQL_VAR_SAMP);
        addLocalAgg(SERIAL_SQL_VAR_SAMP, SERIAL_LOCAL_SQL_VAR_SAMP);
        addIntermediateAgg(SERIAL_SQL_VAR_SAMP, SERIAL_INTERMEDIATE_SQL_VAR_SAMP);
        addIntermediateAgg(SERIAL_LOCAL_SQL_VAR_SAMP, SERIAL_INTERMEDIATE_SQL_VAR_SAMP);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_VAR_SAMP, SERIAL_INTERMEDIATE_SQL_VAR_SAMP);
        addGlobalAgg(SERIAL_SQL_VAR_SAMP, SERIAL_GLOBAL_SQL_VAR_SAMP);

        // SQL VAR_POP

        addAgg(SQL_VAR_POP);
        addAgg(LOCAL_SQL_VAR_POP);
        addAgg(GLOBAL_SQL_VAR_POP);
        addLocalAgg(SQL_VAR_POP, LOCAL_SQL_VAR_POP);
        addIntermediateAgg(SQL_VAR_POP, INTERMEDIATE_SQL_VAR_POP);
        addIntermediateAgg(LOCAL_SQL_VAR_POP, INTERMEDIATE_SQL_VAR_POP);
        addIntermediateAgg(GLOBAL_SQL_VAR_POP, INTERMEDIATE_SQL_VAR_POP);
        addGlobalAgg(SQL_VAR_POP, GLOBAL_SQL_VAR_POP);

        addScalarAgg(SQL_VAR_POP, SCALAR_SQL_VAR_POP);

        addSerialAgg(SQL_VAR_POP, SERIAL_SQL_VAR_POP);
        addSerialAgg(LOCAL_SQL_VAR_POP, SERIAL_LOCAL_SQL_VAR_POP);
        addSerialAgg(GLOBAL_SQL_VAR_POP, SERIAL_GLOBAL_SQL_VAR_POP);
        addAgg(SERIAL_SQL_VAR_POP);
        addAgg(SERIAL_LOCAL_SQL_VAR_POP);
        addAgg(SERIAL_GLOBAL_SQL_VAR_POP);
        addLocalAgg(SERIAL_SQL_VAR_POP, SERIAL_LOCAL_SQL_VAR_POP);
        addIntermediateAgg(SERIAL_SQL_VAR_POP, SERIAL_INTERMEDIATE_SQL_VAR_POP);
        addIntermediateAgg(SERIAL_LOCAL_SQL_VAR_POP, SERIAL_INTERMEDIATE_SQL_VAR_POP);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_VAR_POP, SERIAL_INTERMEDIATE_SQL_VAR_POP);
        addGlobalAgg(SERIAL_SQL_VAR_POP, SERIAL_GLOBAL_SQL_VAR_POP);

        // SQL SKEWNESS

        addAgg(SQL_SKEWNESS);
        addAgg(LOCAL_SQL_SKEWNESS);
        addAgg(GLOBAL_SQL_SKEWNESS);
        addLocalAgg(SQL_SKEWNESS, LOCAL_SQL_SKEWNESS);
        addIntermediateAgg(SQL_SKEWNESS, INTERMEDIATE_SQL_SKEWNESS);
        addIntermediateAgg(LOCAL_SQL_SKEWNESS, INTERMEDIATE_SQL_SKEWNESS);
        addIntermediateAgg(GLOBAL_SQL_SKEWNESS, INTERMEDIATE_SQL_SKEWNESS);
        addGlobalAgg(SQL_SKEWNESS, GLOBAL_SQL_SKEWNESS);

        addScalarAgg(SQL_SKEWNESS, SCALAR_SQL_SKEWNESS);

        addSerialAgg(SQL_SKEWNESS, SERIAL_SQL_SKEWNESS);
        addSerialAgg(LOCAL_SQL_SKEWNESS, SERIAL_LOCAL_SQL_SKEWNESS);
        addSerialAgg(GLOBAL_SQL_SKEWNESS, SERIAL_GLOBAL_SQL_SKEWNESS);
        addAgg(SERIAL_SQL_SKEWNESS);
        addAgg(SERIAL_LOCAL_SQL_SKEWNESS);
        addAgg(SERIAL_GLOBAL_SQL_SKEWNESS);
        addLocalAgg(SERIAL_SQL_SKEWNESS, SERIAL_LOCAL_SQL_SKEWNESS);
        addIntermediateAgg(SERIAL_SQL_SKEWNESS, SERIAL_INTERMEDIATE_SQL_SKEWNESS);
        addIntermediateAgg(SERIAL_LOCAL_SQL_SKEWNESS, SERIAL_INTERMEDIATE_SQL_SKEWNESS);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_SKEWNESS, SERIAL_INTERMEDIATE_SQL_SKEWNESS);
        addGlobalAgg(SERIAL_SQL_SKEWNESS, SERIAL_GLOBAL_SQL_SKEWNESS);

        // SQL KURTOSIS

        addAgg(SQL_KURTOSIS);
        addAgg(LOCAL_SQL_KURTOSIS);
        addAgg(GLOBAL_SQL_KURTOSIS);
        addLocalAgg(SQL_KURTOSIS, LOCAL_SQL_KURTOSIS);
        addIntermediateAgg(SQL_KURTOSIS, INTERMEDIATE_SQL_KURTOSIS);
        addIntermediateAgg(LOCAL_SQL_KURTOSIS, INTERMEDIATE_SQL_KURTOSIS);
        addIntermediateAgg(GLOBAL_SQL_KURTOSIS, INTERMEDIATE_SQL_KURTOSIS);
        addGlobalAgg(SQL_KURTOSIS, GLOBAL_SQL_KURTOSIS);

        addScalarAgg(SQL_KURTOSIS, SCALAR_SQL_KURTOSIS);

        addSerialAgg(SQL_KURTOSIS, SERIAL_SQL_KURTOSIS);
        addSerialAgg(LOCAL_SQL_KURTOSIS, SERIAL_LOCAL_SQL_KURTOSIS);
        addSerialAgg(GLOBAL_SQL_KURTOSIS, SERIAL_GLOBAL_SQL_KURTOSIS);
        addAgg(SERIAL_SQL_KURTOSIS);
        addAgg(SERIAL_LOCAL_SQL_KURTOSIS);
        addAgg(SERIAL_GLOBAL_SQL_KURTOSIS);
        addLocalAgg(SERIAL_SQL_KURTOSIS, SERIAL_LOCAL_SQL_KURTOSIS);
        addIntermediateAgg(SERIAL_SQL_KURTOSIS, SERIAL_INTERMEDIATE_SQL_KURTOSIS);
        addIntermediateAgg(SERIAL_LOCAL_SQL_KURTOSIS, SERIAL_INTERMEDIATE_SQL_KURTOSIS);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_KURTOSIS, SERIAL_INTERMEDIATE_SQL_KURTOSIS);
        addGlobalAgg(SERIAL_SQL_KURTOSIS, SERIAL_GLOBAL_SQL_KURTOSIS);

        // SQL AVG DISTINCT

        addDistinctAgg(SQL_AVG_DISTINCT, SQL_AVG);
        addScalarAgg(SQL_AVG_DISTINCT, SCALAR_SQL_AVG_DISTINCT);

        // SQL STDDEV_SAMP DISTINCT

        addDistinctAgg(SQL_STDDEV_SAMP_DISTINCT, SQL_STDDEV_SAMP);
        addScalarAgg(SQL_STDDEV_SAMP_DISTINCT, SCALAR_SQL_STDDEV_SAMP_DISTINCT);

        // SQL STDDEV_POP DISTINCT

        addDistinctAgg(SQL_STDDEV_POP_DISTINCT, SQL_STDDEV_POP);
        addScalarAgg(SQL_STDDEV_POP_DISTINCT, SCALAR_SQL_STDDEV_POP_DISTINCT);

        // SQL VAR_SAMP DISTINCT

        addDistinctAgg(SQL_VAR_SAMP_DISTINCT, SQL_VAR_SAMP);
        addScalarAgg(SQL_VAR_SAMP_DISTINCT, SCALAR_SQL_VAR_SAMP_DISTINCT);

        // SQL VAR_POP DISTINCT

        addDistinctAgg(SQL_VAR_POP_DISTINCT, SQL_VAR_POP);
        addScalarAgg(SQL_VAR_POP_DISTINCT, SCALAR_SQL_VAR_POP_DISTINCT);

        // SQL SKEWNESS DISTINCT

        addDistinctAgg(SQL_SKEWNESS_DISTINCT, SQL_SKEWNESS);
        addScalarAgg(SQL_SKEWNESS_DISTINCT, SCALAR_SQL_SKEWNESS_DISTINCT);

        // SQL KURTOSIS DISTINCT

        addDistinctAgg(SQL_KURTOSIS_DISTINCT, SQL_KURTOSIS);
        addScalarAgg(SQL_KURTOSIS_DISTINCT, SCALAR_SQL_KURTOSIS_DISTINCT);

        // SQL COUNT

        addAgg(SQL_COUNT);
        addLocalAgg(SQL_COUNT, SQL_COUNT);
        addIntermediateAgg(SQL_COUNT, SQL_SUM);
        addGlobalAgg(SQL_COUNT, SQL_SUM);

        addScalarAgg(SQL_COUNT, SCALAR_SQL_COUNT);

        addSerialAgg(SQL_COUNT, SERIAL_SQL_COUNT);
        addAgg(SERIAL_SQL_COUNT);
        addLocalAgg(SERIAL_SQL_COUNT, SERIAL_SQL_COUNT);
        addIntermediateAgg(SERIAL_SQL_COUNT, SERIAL_SQL_SUM);
        addGlobalAgg(SERIAL_SQL_COUNT, SERIAL_SQL_SUM);

        // SQL COUNT DISTINCT

        addDistinctAgg(SQL_COUNT_DISTINCT, SQL_COUNT);
        addScalarAgg(SQL_COUNT_DISTINCT, SCALAR_SQL_COUNT_DISTINCT);

        // SQL MAX
        addAgg(SQL_MAX);
        addAgg(LOCAL_SQL_MAX);
        addAgg(GLOBAL_SQL_MAX);
        addLocalAgg(SQL_MAX, LOCAL_SQL_MAX);
        addIntermediateAgg(LOCAL_SQL_MAX, INTERMEDIATE_SQL_MAX);
        addIntermediateAgg(GLOBAL_SQL_MAX, GLOBAL_SQL_MAX);
        addIntermediateAgg(SQL_MAX, GLOBAL_SQL_MAX);
        addGlobalAgg(SQL_MAX, GLOBAL_SQL_MAX);

        addScalarAgg(SQL_MAX, SCALAR_SQL_MAX);

        // SQL MAX DISTINCT
        addDistinctAgg(SQL_MAX_DISTINCT, SQL_MAX);
        addScalarAgg(SQL_MAX_DISTINCT, SCALAR_SQL_MAX_DISTINCT);

        // SQL MIN
        addAgg(SQL_MIN);
        addAgg(LOCAL_SQL_MIN);
        addAgg(GLOBAL_SQL_MIN);
        addLocalAgg(SQL_MIN, LOCAL_SQL_MIN);
        addIntermediateAgg(LOCAL_SQL_MIN, INTERMEDIATE_SQL_MIN);
        addIntermediateAgg(GLOBAL_SQL_MIN, GLOBAL_SQL_MIN);
        addIntermediateAgg(SQL_MIN, GLOBAL_SQL_MIN);
        addGlobalAgg(SQL_MIN, GLOBAL_SQL_MIN);

        addScalarAgg(SQL_MIN, SCALAR_SQL_MIN);

        // SQL MIN DISTINCT
        addDistinctAgg(SQL_MIN_DISTINCT, SQL_MIN);
        addScalarAgg(SQL_MIN_DISTINCT, SCALAR_SQL_MIN_DISTINCT);

        // SQL SUM
        addAgg(SQL_SUM);
        addAgg(LOCAL_SQL_SUM);
        addAgg(GLOBAL_SQL_SUM);
        addLocalAgg(SQL_SUM, LOCAL_SQL_SUM);
        addIntermediateAgg(SQL_SUM, INTERMEDIATE_SQL_SUM);
        addIntermediateAgg(LOCAL_SQL_SUM, INTERMEDIATE_SQL_SUM);
        addIntermediateAgg(GLOBAL_SQL_SUM, INTERMEDIATE_SQL_SUM);
        addGlobalAgg(SQL_SUM, GLOBAL_SQL_SUM);
        addScalarAgg(SQL_SUM, SCALAR_SQL_SUM);

        addAgg(SERIAL_SQL_SUM);
        addAgg(SERIAL_LOCAL_SQL_SUM);
        addAgg(SERIAL_GLOBAL_SQL_SUM);
        addSerialAgg(SQL_SUM, SERIAL_SQL_SUM);
        addSerialAgg(LOCAL_SQL_SUM, SERIAL_LOCAL_SQL_SUM);
        addSerialAgg(GLOBAL_SQL_SUM, SERIAL_GLOBAL_SQL_SUM);
        addLocalAgg(SERIAL_SQL_SUM, SERIAL_LOCAL_SQL_SUM);
        addIntermediateAgg(SERIAL_SQL_SUM, SERIAL_SQL_SUM);
        addIntermediateAgg(SERIAL_LOCAL_SQL_SUM, SERIAL_INTERMEDIATE_SQL_SUM);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_SUM, SERIAL_INTERMEDIATE_SQL_SUM);
        addGlobalAgg(SERIAL_SQL_SUM, SERIAL_GLOBAL_SQL_SUM);

        // SQL SUM DISTINCT
        addDistinctAgg(SQL_SUM_DISTINCT, SQL_SUM);
        addScalarAgg(SQL_SUM_DISTINCT, SCALAR_SQL_SUM_DISTINCT);

        // SPATIAL AGGREGATES

        addAgg(ST_UNION_AGG);
        addLocalAgg(ST_UNION_AGG, ST_UNION_AGG);
        addIntermediateAgg(ST_UNION_AGG, ST_UNION_AGG);
        addGlobalAgg(ST_UNION_AGG, ST_UNION_AGG);
        addScalarAgg(ST_UNION_AGG, SCALAR_ST_UNION_AGG);
        addDistinctAgg(ST_UNION_AGG_DISTINCT, ST_UNION_AGG);
        addScalarAgg(ST_UNION_AGG_DISTINCT, SCALAR_ST_UNION_AGG_DISTINCT);

        addAgg(ST_UNION_SQL_AGG);
        addLocalAgg(ST_UNION_SQL_AGG, ST_UNION_SQL_AGG);
        addIntermediateAgg(ST_UNION_SQL_AGG, ST_UNION_SQL_AGG);
        addGlobalAgg(ST_UNION_SQL_AGG, ST_UNION_SQL_AGG);
        addScalarAgg(ST_UNION_SQL_AGG, SCALAR_ST_UNION_SQL_AGG);
        addDistinctAgg(ST_UNION_SQL_AGG_DISTINCT, ST_UNION_SQL_AGG);
        addScalarAgg(ST_UNION_SQL_AGG_DISTINCT, SCALAR_ST_UNION_SQL_AGG_DISTINCT);

        // SQL UNION MBR
        addAgg(SQL_UNION_MBR);
        addAgg(LOCAL_SQL_UNION_MBR);
        addAgg(GLOBAL_SQL_UNION_MBR);
        addLocalAgg(SQL_UNION_MBR, LOCAL_SQL_UNION_MBR);
        addIntermediateAgg(LOCAL_SQL_UNION_MBR, INTERMEDIATE_SQL_UNION_MBR);
        addIntermediateAgg(GLOBAL_SQL_UNION_MBR, GLOBAL_SQL_UNION_MBR);
        addIntermediateAgg(SQL_UNION_MBR, GLOBAL_SQL_UNION_MBR);
        addGlobalAgg(SQL_UNION_MBR, GLOBAL_SQL_UNION_MBR);

        addScalarAgg(SQL_UNION_MBR, SCALAR_SQL_UNION_MBR);
    }

    interface BuiltinFunctionProperty {
    }

    public enum WindowFunctionProperty implements BuiltinFunctionProperty {
        /**
         * Whether the order clause is prohibited
         */
        NO_ORDER_CLAUSE,
        /**
         * Whether the frame clause is prohibited
         */
        NO_FRAME_CLAUSE,
        /**
         * Whether the first argument is a list
         */
        HAS_LIST_ARG,
        /**
         * Whether order by expressions must be injected as arguments
         */
        INJECT_ORDER_ARGS,
        /**
         * Whether a running aggregate requires partition materialization runtime
         */
        MATERIALIZE_PARTITION,
        /**
         * Whether FROM (FIRST | LAST) modifier is allowed
         */
        ALLOW_FROM_FIRST_LAST,
        /**
         * Whether (RESPECT | IGNORE) NULLS modifier is allowed
         */
        ALLOW_RESPECT_IGNORE_NULLS
    }

    static {
        // Window functions
        addWindowFunction(CUME_DIST, CUME_DIST_IMPL, NO_FRAME_CLAUSE, MATERIALIZE_PARTITION);
        addWindowFunction(DENSE_RANK, DENSE_RANK_IMPL, NO_FRAME_CLAUSE, INJECT_ORDER_ARGS);
        addWindowFunction(FIRST_VALUE, FIRST_VALUE_IMPL, HAS_LIST_ARG, ALLOW_RESPECT_IGNORE_NULLS);
        addWindowFunction(LAG, LAG_IMPL, NO_FRAME_CLAUSE, HAS_LIST_ARG, ALLOW_RESPECT_IGNORE_NULLS);
        addWindowFunction(LAST_VALUE, LAST_VALUE_IMPL, HAS_LIST_ARG, ALLOW_RESPECT_IGNORE_NULLS);
        addWindowFunction(LEAD, LEAD_IMPL, NO_FRAME_CLAUSE, HAS_LIST_ARG, ALLOW_RESPECT_IGNORE_NULLS);
        addWindowFunction(NTH_VALUE, NTH_VALUE_IMPL, HAS_LIST_ARG, ALLOW_FROM_FIRST_LAST, ALLOW_RESPECT_IGNORE_NULLS);
        addWindowFunction(NTILE, NTILE_IMPL, NO_FRAME_CLAUSE, MATERIALIZE_PARTITION);
        addWindowFunction(PERCENT_RANK, PERCENT_RANK_IMPL, NO_FRAME_CLAUSE, INJECT_ORDER_ARGS, MATERIALIZE_PARTITION);
        addWindowFunction(RANK, RANK_IMPL, NO_FRAME_CLAUSE, INJECT_ORDER_ARGS);
        addWindowFunction(RATIO_TO_REPORT, RATIO_TO_REPORT_IMPL, HAS_LIST_ARG);
        addWindowFunction(ROW_NUMBER, ROW_NUMBER_IMPL, NO_FRAME_CLAUSE);
        addWindowFunction(null, WIN_MARK_FIRST_MISSING_IMPL, NO_FRAME_CLAUSE, INJECT_ORDER_ARGS);
        addWindowFunction(null, WIN_MARK_FIRST_NULL_IMPL, NO_FRAME_CLAUSE, INJECT_ORDER_ARGS);
        addWindowFunction(null, WIN_PARTITION_LENGTH_IMPL, NO_FRAME_CLAUSE, MATERIALIZE_PARTITION);
    }

    static {
        addUnnestFun(RANGE, true);
        addUnnestFun(SCAN_COLLECTION, false);
        addUnnestFun(SUBSET_COLLECTION, false);
        addUnnestFun(SPATIAL_TILE, false);
    }

    public enum DataSourceFunctionProperty implements BuiltinFunctionProperty {
        /**
         * Force minimum memory budget if a query only uses this function
         */
        MIN_MEMORY_BUDGET
    }

    public static void addDatasourceFunction(FunctionIdentifier fi, IFunctionToDataSourceRewriter transformer,
            DataSourceFunctionProperty... properties) {
        datasourceFunctions.put(fi, transformer);
        registerFunctionProperties(fi, DataSourceFunctionProperty.class, properties);
    }

    public static IFunctionToDataSourceRewriter getDatasourceTransformer(FunctionIdentifier fi) {
        return datasourceFunctions.get(fi);
    }

    public static BuiltinFunctionInfo getBuiltinFunctionInfo(FunctionIdentifier fi) {
        return registeredFunctions.get(fi);
    }

    public static BuiltinFunctionInfo resolveBuiltinFunction(String name, int arity) {
        //TODO:optimize
        BuiltinFunctionInfo finfo;
        finfo = getBuiltinFunctionInfo(FunctionConstants.newAsterix(name, arity));
        if (finfo != null) {
            return finfo;
        }
        finfo = getBuiltinFunctionInfo(FunctionConstants.newAsterix(name, FunctionIdentifier.VARARGS));
        if (finfo != null) {
            return finfo;
        }
        finfo = getBuiltinFunctionInfo(FunctionIdentifier.newAlgebricks(name, arity));
        if (finfo != null) {
            return finfo;
        }
        return getBuiltinFunctionInfo(FunctionIdentifier.newAlgebricks(name, FunctionIdentifier.VARARGS));
    }

    public static boolean isBuiltinAggregateFunction(FunctionIdentifier fi) {
        return builtinAggregateFunctions.contains(fi);
    }

    public static boolean isBuiltinScalarAggregateFunction(FunctionIdentifier fi) {
        return scalarToAggregateFunctionMap.containsKey(fi);
    }

    public static boolean isBuiltinUnnestingFunction(FunctionIdentifier fi) {
        return builtinUnnestingFunctions.containsKey(fi);
    }

    public static boolean returnsUniqueValues(FunctionIdentifier fi) {
        Boolean ruv = builtinUnnestingFunctions.get(fi);
        return ruv != null && ruv;
    }

    public static FunctionIdentifier getIntermediateAggregateFunction(FunctionIdentifier fi) {
        return aggregateToIntermediateAggregate.get(fi);
    }

    public static AggregateFunctionCallExpression makeAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {
        IFunctionInfo finfo = getBuiltinFunctionInfo(fi);
        FunctionIdentifier fiLocal = aggregateToLocalAggregate.get(fi);
        FunctionIdentifier fiGlobal = aggregateToGlobalAggregate.get(fi);

        if (fiLocal != null && fiGlobal != null) {
            AggregateFunctionCallExpression fun = new AggregateFunctionCallExpression(finfo, true, args);
            fun.setStepTwoAggregate(getBuiltinFunctionInfo(fiGlobal));
            fun.setStepOneAggregate(getBuiltinFunctionInfo(fiLocal));
            return fun;
        } else {
            return new AggregateFunctionCallExpression(finfo, false, args);
        }
    }

    public static boolean isAggregateFunctionSerializable(FunctionIdentifier fi) {
        return aggregateToSerializableAggregate.containsKey(fi);
    }

    public static AggregateFunctionCallExpression makeSerializableAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {

        FunctionIdentifier serializableFi = aggregateToSerializableAggregate.get(fi);
        if (serializableFi == null) {
            throw new IllegalStateException("no serializable implementation for aggregate function " + fi);
        }
        IFunctionInfo serializableFinfo = getBuiltinFunctionInfo(serializableFi);

        FunctionIdentifier fiLocal = aggregateToLocalAggregate.get(serializableFi);
        FunctionIdentifier fiGlobal = aggregateToGlobalAggregate.get(serializableFi);

        if (fiLocal != null && fiGlobal != null) {
            AggregateFunctionCallExpression fun = new AggregateFunctionCallExpression(serializableFinfo, true, args);
            fun.setStepTwoAggregate(getBuiltinFunctionInfo(fiGlobal));
            fun.setStepOneAggregate(getBuiltinFunctionInfo(fiLocal));
            return fun;
        } else {
            return new AggregateFunctionCallExpression(serializableFinfo, false, args);
        }
    }

    public static FunctionIdentifier getAggregateFunction(FunctionIdentifier scalarVersionOfAggregate) {
        return scalarToAggregateFunctionMap.get(scalarVersionOfAggregate);
    }

    public static FunctionIdentifier getAggregateFunctionForDistinct(FunctionIdentifier distinctVersionOfAggregate) {
        return distinctToRegularAggregateFunctionMap.get(distinctVersionOfAggregate);
    }

    public static void addFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer, boolean isFunctional) {
        addFunction(new BuiltinFunctionInfo(fi, typeComputer, isFunctional, false));
    }

    public static void addPrivateFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer,
            boolean isFunctional) {
        addFunction(new BuiltinFunctionInfo(fi, typeComputer, isFunctional, true));
    }

    private static void addFunction(BuiltinFunctionInfo functionInfo) {
        registeredFunctions.put(functionInfo.getFunctionIdentifier(), functionInfo);
    }

    private static <T extends Enum<T> & BuiltinFunctionProperty> void registerFunctionProperties(FunctionIdentifier fid,
            Class<T> propertyClass, T[] properties) {
        if (properties == null) {
            return;
        }
        Set<T> propertySet = EnumSet.noneOf(propertyClass);
        Collections.addAll(propertySet, properties);
        builtinFunctionProperties.put(fid, propertySet);
    }

    public static boolean builtinFunctionHasProperty(FunctionIdentifier fi, BuiltinFunctionProperty property) {
        Set<? extends BuiltinFunctionProperty> propertySet = builtinFunctionProperties.get(fi);
        return propertySet != null && propertySet.contains(property);
    }

    public static void addAgg(FunctionIdentifier fi) {
        builtinAggregateFunctions.add(fi);
    }

    public static void addLocalAgg(FunctionIdentifier fi, FunctionIdentifier localfi) {
        aggregateToLocalAggregate.put(fi, localfi);
    }

    public static void addIntermediateAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToIntermediateAggregate.put(fi, globalfi);
    }

    public static void addGlobalAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToGlobalAggregate.put(fi, globalfi);
        globalAggregateFunctions.add(globalfi);
    }

    public static void addUnnestFun(FunctionIdentifier fi, boolean returnsUniqueValues) {
        builtinUnnestingFunctions.put(fi, returnsUniqueValues);
    }

    public static void addSerialAgg(FunctionIdentifier fi, FunctionIdentifier serialfi) {
        aggregateToSerializableAggregate.put(fi, serialfi);
    }

    public static void addScalarAgg(FunctionIdentifier fi, FunctionIdentifier scalarfi) {
        scalarToAggregateFunctionMap.put(scalarfi, fi);
    }

    public static void addDistinctAgg(FunctionIdentifier distinctfi, FunctionIdentifier fi) {
        distinctToRegularAggregateFunctionMap.put(distinctfi, fi);
    }

    public static void addWindowFunction(FunctionIdentifier sqlfi, FunctionIdentifier winfi,
            WindowFunctionProperty... properties) {
        if (sqlfi != null) {
            sqlToWindowFunctions.put(sqlfi, winfi);
        }
        windowFunctions.add(winfi);
        registerFunctionProperties(winfi, WindowFunctionProperty.class, properties);
    }

    public static FunctionIdentifier getWindowFunction(FunctionIdentifier sqlfi) {
        return sqlToWindowFunctions.get(sqlfi);
    }

    public static boolean isWindowFunction(FunctionIdentifier winfi) {
        return windowFunctions.contains(winfi);
    }

    public static AbstractFunctionCallExpression makeWindowFunctionExpression(FunctionIdentifier winfi,
            List<Mutable<ILogicalExpression>> args) {
        IFunctionInfo finfo = getBuiltinFunctionInfo(winfi);
        if (finfo == null) {
            throw new IllegalStateException("no implementation for window function " + winfi);
        }
        return new StatefulFunctionCallExpression(finfo, UnpartitionedPropertyComputer.INSTANCE, args);
    }

    public enum SpatialFilterKind {
        SI,
        STFR
    }

    static {
        spatialFilterFunctions.put(BuiltinFunctions.SPATIAL_INTERSECT, SpatialFilterKind.SI);
        spatialFilterFunctions.put(BuiltinFunctions.ST_INTERSECTS, SpatialFilterKind.STFR);
        spatialFilterFunctions.put(BuiltinFunctions.ST_OVERLAPS, SpatialFilterKind.STFR);
        spatialFilterFunctions.put(BuiltinFunctions.ST_TOUCHES, SpatialFilterKind.STFR);
        spatialFilterFunctions.put(BuiltinFunctions.ST_CONTAINS, SpatialFilterKind.STFR);
        spatialFilterFunctions.put(BuiltinFunctions.ST_CROSSES, SpatialFilterKind.STFR);
        spatialFilterFunctions.put(BuiltinFunctions.ST_WITHIN, SpatialFilterKind.STFR);
    }

    public static boolean isGlobalAggregateFunction(FunctionIdentifier fi) {
        return globalAggregateFunctions.contains(fi);
    }

    public static boolean isSpatialFilterFunction(FunctionIdentifier fi) {
        return spatialFilterFunctions.get(fi) == SpatialFilterKind.SI;
    }

    public static boolean isSTFilterRefineFunction(FunctionIdentifier fi) {
        return spatialFilterFunctions.get(fi) == SpatialFilterKind.STFR;
    }

    static {
        similarityFunctions.add(SIMILARITY_JACCARD);
        similarityFunctions.add(SIMILARITY_JACCARD_CHECK);
        similarityFunctions.add(EDIT_DISTANCE);
        similarityFunctions.add(EDIT_DISTANCE_CHECK);
        similarityFunctions.add(EDIT_DISTANCE_CONTAINS);
    }

    public static boolean isSimilarityFunction(FunctionIdentifier fi) {
        return similarityFunctions.contains(fi);
    }
}
