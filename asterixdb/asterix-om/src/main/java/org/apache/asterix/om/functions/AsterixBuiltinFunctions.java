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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABinaryTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ACircleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADateTimeTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADateTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADayTimeDurationTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ADurationTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AFloatTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt16TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt32TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AInt8TypeComputer;
import org.apache.asterix.om.typecomputer.impl.AIntervalTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ALineTypeComputer;
import org.apache.asterix.om.typecomputer.impl.AMissingTypeComputer;
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
import org.apache.asterix.om.typecomputer.impl.BooleanFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BooleanOnlyTypeComputer;
import org.apache.asterix.om.typecomputer.impl.BooleanOrMissingTypeComputer;
import org.apache.asterix.om.typecomputer.impl.CastTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ClosedRecordConstructorResultType;
import org.apache.asterix.om.typecomputer.impl.CollectionMemberResultType;
import org.apache.asterix.om.typecomputer.impl.CollectionToSequenceTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ConcatNonNullTypeComputer;
import org.apache.asterix.om.typecomputer.impl.FieldAccessByIndexResultType;
import org.apache.asterix.om.typecomputer.impl.FieldAccessByNameResultType;
import org.apache.asterix.om.typecomputer.impl.FieldAccessNestedResultType;
import org.apache.asterix.om.typecomputer.impl.GetOverlappingInvervalTypeComputer;
import org.apache.asterix.om.typecomputer.impl.InjectFailureTypeComputer;
import org.apache.asterix.om.typecomputer.impl.LocalAvgTypeComputer;
import org.apache.asterix.om.typecomputer.impl.MinMaxAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NonTaggedGetItemResultType;
import org.apache.asterix.om.typecomputer.impl.NotUnknownTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NullableDoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericAddSubMulDivTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericDoubleOutputFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericInt8OutputFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericRound2TypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericUnaryFunctionTypeComputer;
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
import org.apache.asterix.om.typecomputer.impl.RecordMergeTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordPairsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.RecordRemoveFieldsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ScalarVersionOfAggregateResultType;
import org.apache.asterix.om.typecomputer.impl.StringBooleanTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringInt32TypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringIntToStringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringStringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringToInt64ListTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringToStringListTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SubsetCollectionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SubstringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SwitchCaseComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryBinaryInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryMinusTypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryStringInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnorderedListConstructorTypeComputer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class AsterixBuiltinFunctions {

    public enum SpatialFilterKind {
        SI
    }

    private static final FunctionInfoRepository registeredFunctions = new FunctionInfoRepository();

    private static final Map<IFunctionInfo, ATypeHierarchy.Domain> registeredFunctionsDomain = new HashMap<>();

    // it is supposed to be an identity mapping
    private static final Map<IFunctionInfo, IFunctionInfo> builtinPublicFunctionsSet = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> builtinPrivateFunctionsSet = new HashMap<>();

    private static final Map<IFunctionInfo, IResultTypeComputer> funTypeComputer = new HashMap<>();

    private static final Set<IFunctionInfo> builtinAggregateFunctions = new HashSet<>();
    private static final Set<IFunctionInfo> datasetFunctions = new HashSet<>();
    private static final Set<IFunctionInfo> similarityFunctions = new HashSet<>();
    private static final Set<IFunctionInfo> globalAggregateFunctions = new HashSet<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToLocalAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToIntermediateAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToGlobalAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToSerializableAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, Boolean> builtinUnnestingFunctions = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> scalarToAggregateFunctionMap = new HashMap<>();
    private static final Map<IFunctionInfo, SpatialFilterKind> spatialFilterFunctions = new HashMap<>();

    public static final FunctionIdentifier TYPE_OF = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "type-of", 1);
    public static final FunctionIdentifier GET_HANDLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-handle", 2);
    public static final FunctionIdentifier GET_DATA = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-data",
            2);

    public static final FunctionIdentifier GET_ITEM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-item",
            2);
    public static final FunctionIdentifier ANY_COLLECTION_MEMBER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "any-collection-member", 1);
    public static final FunctionIdentifier LISTIFY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "listify", 1);
    public static final FunctionIdentifier LEN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "len", 1);

    public static final FunctionIdentifier CONCAT_NON_NULL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "concat-non-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier EMPTY_STREAM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "empty-stream", 0);
    public static final FunctionIdentifier NON_EMPTY_STREAM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "non-empty-stream", 0);
    public static final FunctionIdentifier ORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "ordered-list-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier UNORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unordered-list-constructor", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier DEEP_EQUAL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "deep-equal", 2);

    // objects
    public static final FunctionIdentifier RECORD_MERGE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "object-merge", 2);
    public static final FunctionIdentifier REMOVE_FIELDS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "object-remove-fields", 2);
    public static final FunctionIdentifier ADD_FIELDS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "object-add-fields", 2);

    public static final FunctionIdentifier CLOSED_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "closed-object-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier OPEN_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "open-object-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier FIELD_ACCESS_BY_INDEX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "field-access-by-index", 2);
    public static final FunctionIdentifier FIELD_ACCESS_BY_NAME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "field-access-by-name", 2);
    public static final FunctionIdentifier FIELD_ACCESS_NESTED = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "field-access-nested", 2);
    public static final FunctionIdentifier GET_RECORD_FIELDS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-object-fields", 1);
    public static final FunctionIdentifier GET_RECORD_FIELD_VALUE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-object-field-value", 2);
    public static final FunctionIdentifier RECORD_PAIRS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "object-pairs", FunctionIdentifier.VARARGS);

    // numeric
    public static final FunctionIdentifier NUMERIC_UNARY_MINUS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-unary-minus", 1);
    public static final FunctionIdentifier NUMERIC_SUBTRACT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-subtract", 2);
    public static final FunctionIdentifier NUMERIC_MULTIPLY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-multiply", 2);
    public static final FunctionIdentifier NUMERIC_DIVIDE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-divide", 2);
    public static final FunctionIdentifier NUMERIC_MOD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-mod", 2);
    public static final FunctionIdentifier NUMERIC_IDIV = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-idiv", 2);
    public static final FunctionIdentifier CARET = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "caret", 2);
    public static final FunctionIdentifier NUMERIC_ABS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "abs", 1);
    public static final FunctionIdentifier NUMERIC_ACOS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "acos",
            1);
    public static final FunctionIdentifier NUMERIC_ASIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "asin",
            1);
    public static final FunctionIdentifier NUMERIC_ATAN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "atan",
            1);
    public static final FunctionIdentifier NUMERIC_ATAN2 = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "atan2",
            2);
    public static final FunctionIdentifier NUMERIC_COS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cos", 1);
    public static final FunctionIdentifier NUMERIC_SIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sin", 1);
    public static final FunctionIdentifier NUMERIC_TAN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tan", 1);
    public static final FunctionIdentifier NUMERIC_EXP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "exp", 1);
    public static final FunctionIdentifier NUMERIC_LN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ln", 1);
    public static final FunctionIdentifier NUMERIC_LOG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "log", 1);
    public static final FunctionIdentifier NUMERIC_SQRT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sqrt",
            1);
    public static final FunctionIdentifier NUMERIC_SIGN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sign",
            1);

    public static final FunctionIdentifier NUMERIC_CEILING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ceiling", 1);
    public static final FunctionIdentifier NUMERIC_FLOOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "floor",
            1);
    public static final FunctionIdentifier NUMERIC_ROUND = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "round",
            1);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "round-half-to-even", 1);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN2 = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "round-half-to-even", 2);
    public static final FunctionIdentifier NUMERIC_TRUNC = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "trunc",
            2);

    // binary functions
    public static final FunctionIdentifier BINARY_LENGTH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "binary-length", 1);
    public static final FunctionIdentifier PARSE_BINARY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-binary", 2);
    public static final FunctionIdentifier PRINT_BINARY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-binary", 2);
    public static final FunctionIdentifier BINARY_CONCAT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "binary-concat", 1);
    public static final FunctionIdentifier SUBBINARY_FROM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sub-binary", 2);
    public static final FunctionIdentifier SUBBINARY_FROM_TO = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sub-binary", 3);
    public static final FunctionIdentifier FIND_BINARY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "find-binary", 2);
    public static final FunctionIdentifier FIND_BINARY_FROM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "find-binary", 3);
    // String funcitons
    public static final FunctionIdentifier STRING_EQUAL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-equal", 2);
    public static final FunctionIdentifier STRING_MATCHES = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "matches", 2);
    public static final FunctionIdentifier STRING_MATCHES_WITH_FLAG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "matches", 3);
    public static final FunctionIdentifier STRING_REGEXP_LIKE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "regexp-like", 2);
    public static final FunctionIdentifier STRING_REGEXP_LIKE_WITH_FLAG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "regexp-like", 3);
    public static final FunctionIdentifier STRING_REGEXP_POSITION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "regexp-position", 2);
    public static final FunctionIdentifier STRING_REGEXP_POSITION_WITH_FLAG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "regexp-position", 3);
    public static final FunctionIdentifier STRING_LOWERCASE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "lowercase", 1);
    public static final FunctionIdentifier STRING_UPPERCASE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "uppercase", 1);
    public static final FunctionIdentifier STRING_INITCAP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "initcap", 1);
    public static final FunctionIdentifier STRING_TRIM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "trim",
            1);
    public static final FunctionIdentifier STRING_LTRIM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ltrim",
            1);
    public static final FunctionIdentifier STRING_RTRIM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "rtrim",
            1);
    public static final FunctionIdentifier STRING_TRIM2 = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "trim",
            2);
    public static final FunctionIdentifier STRING_LTRIM2 = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ltrim", 2);
    public static final FunctionIdentifier STRING_RTRIM2 = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "rtrim", 2);
    public static final FunctionIdentifier STRING_POSITION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "position", 2);
    public static final FunctionIdentifier STRING_REPLACE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "replace", 3);
    public static final FunctionIdentifier STRING_REPLACE_WITH_FLAG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "replace", 4);
    public static final FunctionIdentifier STRING_LENGTH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-length", 1);
    public static final FunctionIdentifier STRING_LIKE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "like",
            2);
    public static final FunctionIdentifier STRING_CONTAINS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "contains", 2);
    public static final FunctionIdentifier STRING_STARTS_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "starts-with", 2);
    public static final FunctionIdentifier STRING_ENDS_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ends-with", 2);
    public static final FunctionIdentifier SUBSTRING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "substring",
            3);
    public static final FunctionIdentifier SUBSTRING2 = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring", 2);
    public static final FunctionIdentifier SUBSTRING_BEFORE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring-before", 2);
    public static final FunctionIdentifier SUBSTRING_AFTER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring-after", 2);
    public static final FunctionIdentifier STRING_TO_CODEPOINT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-to-codepoint", 1);
    public static final FunctionIdentifier CODEPOINT_TO_STRING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "codepoint-to-string", 1);
    public static final FunctionIdentifier STRING_CONCAT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-concat", 1);
    public static final FunctionIdentifier STRING_JOIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-join", 2);
    public static final FunctionIdentifier STRING_REPEAT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "repeat", 2);
    public static final FunctionIdentifier STRING_SPLIT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "split",
            2);

    public static final FunctionIdentifier DATASET = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "dataset", 1);
    public static final FunctionIdentifier FEED_COLLECT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "feed-collect", 6);
    public static final FunctionIdentifier FEED_INTERCEPT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "feed-intercept", 1);

    public static final FunctionIdentifier INDEX_SEARCH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "index-search", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier MAKE_FIELD_INDEX_HANDLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "make-field-index-handle", 2);
    public static final FunctionIdentifier MAKE_FIELD_NESTED_HANDLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "make-field-nested-handle", 3);
    public static final FunctionIdentifier MAKE_FIELD_NAME_HANDLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "make-field-name-handle", 1);

    public static final FunctionIdentifier AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-avg", 1);
    public static final FunctionIdentifier COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count", 1);
    public static final FunctionIdentifier SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sum", 1);
    public static final FunctionIdentifier LOCAL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-sum", 1);
    public static final FunctionIdentifier MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-max", 1);
    public static final FunctionIdentifier LOCAL_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-max", 1);
    public static final FunctionIdentifier MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-min", 1);
    public static final FunctionIdentifier LOCAL_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-min", 1);
    public static final FunctionIdentifier GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-global-avg", 1);
    public static final FunctionIdentifier INTERMEDIATE_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-intermediate-avg", 1);
    public static final FunctionIdentifier LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-avg", 1);
    public static final FunctionIdentifier FIRST_ELEMENT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-first-element", 1);
    public static final FunctionIdentifier LOCAL_FIRST_ELEMENT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-first-element", 1);

    public static final FunctionIdentifier SCALAR_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "avg", 1);
    public static final FunctionIdentifier SCALAR_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "count",
            1);
    public static final FunctionIdentifier SCALAR_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sum", 1);
    public static final FunctionIdentifier SCALAR_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "max", 1);
    public static final FunctionIdentifier SCALAR_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "min", 1);
    public static final FunctionIdentifier SCALAR_GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-avg", 1);
    public static final FunctionIdentifier SCALAR_LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-avg", 1);
    public static final FunctionIdentifier SCALAR_FIRST_ELEMENT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "first-element", 1);

    // serializable aggregate functions
    public static final FunctionIdentifier SERIAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "avg-serial", 1);
    public static final FunctionIdentifier SERIAL_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "count-serial", 1);
    public static final FunctionIdentifier SERIAL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sum-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_AVG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "intermediate-avg-serial", 1);

    // sql aggregate functions
    public static final FunctionIdentifier SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-avg",
            1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "intermediate-agg-sql-avg", 1);
    public static final FunctionIdentifier SQL_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-sql-count", 1);
    public static final FunctionIdentifier SQL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-sum",
            1);
    public static final FunctionIdentifier LOCAL_SQL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-sql-sum", 1);
    public static final FunctionIdentifier SQL_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-max",
            1);
    public static final FunctionIdentifier LOCAL_SQL_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-sql-max", 1);
    public static final FunctionIdentifier SQL_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-min",
            1);
    public static final FunctionIdentifier LOCAL_SQL_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-sql-min", 1);
    public static final FunctionIdentifier GLOBAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-global-sql-avg", 1);
    public static final FunctionIdentifier LOCAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-sql-avg", 1);

    public static final FunctionIdentifier SCALAR_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-avg", 1);
    public static final FunctionIdentifier SCALAR_SQL_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-count", 1);
    public static final FunctionIdentifier SCALAR_SQL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-sum", 1);
    public static final FunctionIdentifier SCALAR_SQL_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-max", 1);
    public static final FunctionIdentifier SCALAR_SQL_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-min", 1);
    public static final FunctionIdentifier SCALAR_GLOBAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-sql-avg", 1);
    public static final FunctionIdentifier SCALAR_LOCAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-sql-avg", 1);

    // serializable sql aggregate functions
    public static final FunctionIdentifier SERIAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-count-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_AVG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "intermediate-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-sql-avg-serial", 1);

    public static final FunctionIdentifier SCAN_COLLECTION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "scan-collection", 1);
    public static final FunctionIdentifier SUBSET_COLLECTION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "subset-collection", 3);

    public static final FunctionIdentifier RANGE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "range", 2);

    // fuzzy functions:
    public static final FunctionIdentifier FUZZY_EQ = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "fuzzy-eq",
            2);

    public static final FunctionIdentifier PREFIX_LEN_JACCARD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "prefix-len-jaccard", 2);

    public static final FunctionIdentifier SIMILARITY_JACCARD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "similarity-jaccard", 2);
    public static final FunctionIdentifier SIMILARITY_JACCARD_CHECK = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-check", 3);
    public static final FunctionIdentifier SIMILARITY_JACCARD_SORTED = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-sorted", 2);
    public static final FunctionIdentifier SIMILARITY_JACCARD_SORTED_CHECK = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-sorted-check", 3);
    public static final FunctionIdentifier SIMILARITY_JACCARD_PREFIX = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-prefix", 6);
    public static final FunctionIdentifier SIMILARITY_JACCARD_PREFIX_CHECK = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-prefix-check", 6);

    public static final FunctionIdentifier EDIT_DISTANCE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance", 2);
    public static final FunctionIdentifier EDIT_DISTANCE_CHECK = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance-check", 3);
    public static final FunctionIdentifier EDIT_DISTANCE_LIST_IS_FILTERABLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "edit-distance-list-is-filterable", 2);
    public static final FunctionIdentifier EDIT_DISTANCE_STRING_IS_FILTERABLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "edit-distance-string-is-filterable", 4);
    public static final FunctionIdentifier EDIT_DISTANCE_CONTAINS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance-contains", 3);

    // tokenizers:
    public static final FunctionIdentifier WORD_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "word-tokens", 1);
    public static final FunctionIdentifier HASHED_WORD_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "hashed-word-tokens", 1);
    public static final FunctionIdentifier COUNTHASHED_WORD_TOKENS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "counthashed-word-tokens", 1);
    public static final FunctionIdentifier GRAM_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "gram-tokens", 3);
    public static final FunctionIdentifier HASHED_GRAM_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "hashed-gram-tokens", 3);
    public static final FunctionIdentifier COUNTHASHED_GRAM_TOKENS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "counthashed-gram-tokens", 3);

    public static final FunctionIdentifier TID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tid", 0);
    public static final FunctionIdentifier GTID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "gtid", 0);

    // constructors:
    public static final FunctionIdentifier BOOLEAN_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "boolean", 1);
    public static final FunctionIdentifier NULL_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "null", 1);
    public static final FunctionIdentifier STRING_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string", 1);
    public static final FunctionIdentifier BINARY_HEX_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "hex", 1);
    public static final FunctionIdentifier BINARY_BASE64_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "base64", 1);
    public static final FunctionIdentifier INT8_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int8", 1);
    public static final FunctionIdentifier INT16_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int16", 1);
    public static final FunctionIdentifier INT32_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int32", 1);
    public static final FunctionIdentifier INT64_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int64", 1);
    public static final FunctionIdentifier FLOAT_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "float", 1);
    public static final FunctionIdentifier DOUBLE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "double", 1);
    public static final FunctionIdentifier POINT_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "point", 1);
    public static final FunctionIdentifier POINT3D_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "point3d", 1);
    public static final FunctionIdentifier LINE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "line", 1);
    public static final FunctionIdentifier CIRCLE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "circle", 1);
    public static final FunctionIdentifier RECTANGLE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "rectangle", 1);
    public static final FunctionIdentifier POLYGON_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "polygon", 1);
    public static final FunctionIdentifier TIME_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "time", 1);
    public static final FunctionIdentifier DATE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "date", 1);
    public static final FunctionIdentifier DATETIME_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "datetime", 1);
    public static final FunctionIdentifier DURATION_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration", 1);
    public static final FunctionIdentifier UUID_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "uuid", 1);

    public static final FunctionIdentifier YEAR_MONTH_DURATION_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "year-month-duration", 1);
    public static final FunctionIdentifier DAY_TIME_DURATION_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "day-time-duration", 1);

    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-start-from-date", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-start-from-time", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-start-from-datetime", 2);
    public static final FunctionIdentifier INTERVAL_BEFORE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-before", 2);
    public static final FunctionIdentifier INTERVAL_AFTER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-after", 2);
    public static final FunctionIdentifier INTERVAL_MEETS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-meets", 2);
    public static final FunctionIdentifier INTERVAL_MET_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-met-by", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-overlaps", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPPED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-overlapped-by", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPPING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-overlapping", 2);
    public static final FunctionIdentifier INTERVAL_STARTS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-starts", 2);
    public static final FunctionIdentifier INTERVAL_STARTED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-started-by", 2);
    public static final FunctionIdentifier INTERVAL_COVERS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-covers", 2);
    public static final FunctionIdentifier INTERVAL_COVERED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-covered-by", 2);
    public static final FunctionIdentifier INTERVAL_ENDS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-ends", 2);
    public static final FunctionIdentifier INTERVAL_ENDED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-ended-by", 2);
    public static final FunctionIdentifier CURRENT_TIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "current-time", 0);
    public static final FunctionIdentifier CURRENT_DATE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "current-date", 0);
    public static final FunctionIdentifier CURRENT_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "current-datetime", 0);
    public static final FunctionIdentifier DURATION_EQUAL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration-equal", 2);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_GREATER_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "year-month-duration-greater-than", 2);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_LESS_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "year-month-duration-less-than", 2);
    public static final FunctionIdentifier DAY_TIME_DURATION_GREATER_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "day-time-duration-greater-than", 2);
    public static final FunctionIdentifier DAY_TIME_DURATION_LESS_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "day-time-duration-less-than", 2);
    public static final FunctionIdentifier DURATION_FROM_MONTHS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration-from-months", 1);
    public static final FunctionIdentifier MONTHS_FROM_YEAR_MONTH_DURATION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "months-from-year-month-duration", 1);
    public static final FunctionIdentifier DURATION_FROM_MILLISECONDS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "duration-from-ms", 1);
    public static final FunctionIdentifier MILLISECONDS_FROM_DAY_TIME_DURATION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "ms-from-day-time-duration", 1);

    public static final FunctionIdentifier GET_YEAR_MONTH_DURATION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-year-month-duration", 1);
    public static final FunctionIdentifier GET_DAY_TIME_DURATION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-day-time-duration", 1);
    public static final FunctionIdentifier DURATION_FROM_INTERVAL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration-from-interval", 1);

    // spatial
    public static final FunctionIdentifier CREATE_POINT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-point", 2);
    public static final FunctionIdentifier CREATE_LINE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-line", 2);
    public static final FunctionIdentifier CREATE_POLYGON = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-polygon", 1);
    public static final FunctionIdentifier CREATE_CIRCLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-circle", 2);
    public static final FunctionIdentifier CREATE_RECTANGLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-rectangle", 2);
    public static final FunctionIdentifier SPATIAL_INTERSECT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-intersect", 2);
    public static final FunctionIdentifier SPATIAL_AREA = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-area", 1);
    public static final FunctionIdentifier SPATIAL_DISTANCE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-distance", 2);
    public static final FunctionIdentifier CREATE_MBR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-mbr", 3);
    public static final FunctionIdentifier SPATIAL_CELL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-cell", 4);
    public static final FunctionIdentifier SWITCH_CASE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "switch-case", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier INJECT_FAILURE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "inject-failure", 2);
    public static final FunctionIdentifier FLOW_RECORD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "flow-object", 1);
    public static final FunctionIdentifier CAST_TYPE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "cast", 1);

    public static final FunctionIdentifier CREATE_UUID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-uuid", 0);
    public static final FunctionIdentifier UUID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "uuid", 0);
    public static final FunctionIdentifier CREATE_QUERY_UID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-query-uid", 0);

    // Spatial and temporal type accessors
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_YEAR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-year", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MONTH = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-month", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_DAY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-day", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_HOUR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-hour", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-minute", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_SEC = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-second", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MILLISEC = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-millisecond", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-start", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-end", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-start-datetime", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-end-datetime", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-start-date", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-end-date", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-start-time", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-interval-end-time", 1);
    public static final FunctionIdentifier INTERVAL_BIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-bin", 3);
    public static final FunctionIdentifier OVERLAP_BINS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "overlap-bins", 3);
    public static final FunctionIdentifier GET_OVERLAPPING_INTERVAL = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-overlapping-interval", 2);

    // Temporal functions
    public static final FunctionIdentifier UNIX_TIME_FROM_DATE_IN_DAYS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unix-time-from-date-in-days", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_TIME_IN_MS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unix-time-from-time-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_MS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unix-time-from-datetime-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_SECS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unix-time-from-datetime-in-secs", 1);
    public static final FunctionIdentifier DATE_FROM_UNIX_TIME_IN_DAYS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "date-from-unix-time-in-days", 1);
    public static final FunctionIdentifier DATE_FROM_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-date-from-datetime", 1);
    public static final FunctionIdentifier TIME_FROM_UNIX_TIME_IN_MS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "time-from-unix-time-in-ms", 1);
    public static final FunctionIdentifier TIME_FROM_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-time-from-datetime", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_MS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "datetime-from-unix-time-in-ms", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_SECS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "datetime-from-unix-time-in-secs", 1);
    public static final FunctionIdentifier DATETIME_FROM_DATE_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "datetime-from-date-time", 2);
    public static final FunctionIdentifier CALENDAR_DURATION_FROM_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "calendar-duration-from-datetime", 2);
    public static final FunctionIdentifier CALENDAR_DURATION_FROM_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "calendar-duration-from-date", 2);
    public static final FunctionIdentifier ADJUST_TIME_FOR_TIMEZONE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "adjust-time-for-timezone", 2);
    public static final FunctionIdentifier ADJUST_DATETIME_FOR_TIMEZONE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "adjust-datetime-for-timezone", 2);
    public static final FunctionIdentifier DAY_OF_WEEK = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "day-of-week", 1);
    public static final FunctionIdentifier PARSE_DATE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-date", 2);
    public static final FunctionIdentifier PARSE_TIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-time", 2);
    public static final FunctionIdentifier PARSE_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-datetime", 2);
    public static final FunctionIdentifier PRINT_DATE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-date", 2);
    public static final FunctionIdentifier PRINT_TIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-time", 2);
    public static final FunctionIdentifier PRINT_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-datetime", 2);

    public static final FunctionIdentifier GET_POINT_X_COORDINATE_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-x", 1);
    public static final FunctionIdentifier GET_POINT_Y_COORDINATE_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-y", 1);
    public static final FunctionIdentifier GET_CIRCLE_RADIUS_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-radius", 1);
    public static final FunctionIdentifier GET_CIRCLE_CENTER_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-center", 1);
    public static final FunctionIdentifier GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-points", 1);

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
    public static final FunctionIdentifier IS_UNKOWN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "is-unknown", 1);
    public static final FunctionIdentifier IS_BOOLEAN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "is-boolean", 1);
    public static final FunctionIdentifier IS_NUMBER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-number",
            1);
    public static final FunctionIdentifier IS_STRING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-string",
            1);
    public static final FunctionIdentifier IS_ARRAY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-array",
            1);
    public static final FunctionIdentifier IS_OBJECT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-object",
            1);

    public static final FunctionIdentifier IS_SYSTEM_NULL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "is-system-null", 1);
    public static final FunctionIdentifier CHECK_UNKNOWN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "check-unknown", 1);
    public static final FunctionIdentifier COLLECTION_TO_SEQUENCE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "" + "collection-to-sequence", 1);

    public static final FunctionIdentifier EXTERNAL_LOOKUP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "external-lookup", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier META = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "meta",
            FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier META_KEY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "meta-key",
            FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RESOLVE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "resolve",
            FunctionIdentifier.VARARGS);

    public static IFunctionInfo getAsterixFunctionInfo(FunctionIdentifier fid) {
        return registeredFunctions.get(fid);
    }

    public static AsterixFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return (AsterixFunctionInfo) registeredFunctions.get(fid);
    }

    static {

        // first, take care of Algebricks builtin functions
        addFunction(IS_MISSING, BooleanOnlyTypeComputer.INSTANCE, true);
        addFunction(IS_UNKOWN, BooleanOnlyTypeComputer.INSTANCE, true);
        addFunction(IS_NULL, BooleanOrMissingTypeComputer.INSTANCE, true);
        addFunction(IS_SYSTEM_NULL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_BOOLEAN, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_NUMBER, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_STRING, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_ARRAY, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_OBJECT, ABooleanTypeComputer.INSTANCE, true);
        addFunction(NOT, ABooleanTypeComputer.INSTANCE, true);

        addPrivateFunction(EQ, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(LE, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(GE, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(LT, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(GT, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(AND, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(NEQ, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(OR, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_ADD, NumericAddSubMulDivTypeComputer.INSTANCE, true);

        // Deep equality
        addFunction(DEEP_EQUAL, BooleanFunctionTypeComputer.INSTANCE, true);

        // and then, Asterix builtin functions
        addPrivateFunction(CHECK_UNKNOWN, NotUnknownTypeComputer.INSTANCE, true);
        addPrivateFunction(ANY_COLLECTION_MEMBER, CollectionMemberResultType.INSTANCE, true);
        addFunction(BOOLEAN_CONSTRUCTOR, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(CARET, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addFunction(CIRCLE_CONSTRUCTOR, ACircleTypeComputer.INSTANCE, true);
        addPrivateFunction(CONCAT_NON_NULL, ConcatNonNullTypeComputer.INSTANCE, true);

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
        addFunction(CREATE_QUERY_UID, ABinaryTypeComputer.INSTANCE, false);
        addFunction(UUID_CONSTRUCTOR, AUUIDTypeComputer.INSTANCE, true);

        addFunction(DATE_CONSTRUCTOR, ADateTypeComputer.INSTANCE, true);
        addFunction(DATETIME_CONSTRUCTOR, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(DOUBLE_CONSTRUCTOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(DURATION_CONSTRUCTOR, ADurationTypeComputer.INSTANCE, true);
        addFunction(YEAR_MONTH_DURATION_CONSTRUCTOR, AYearMonthDurationTypeComputer.INSTANCE, true);
        addFunction(DAY_TIME_DURATION_CONSTRUCTOR, ADayTimeDurationTypeComputer.INSTANCE, true);
        addFunction(EDIT_DISTANCE, AInt64TypeComputer.INSTANCE, true);
        addFunction(EDIT_DISTANCE_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addPrivateFunction(EDIT_DISTANCE_STRING_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(EDIT_DISTANCE_LIST_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE, true);
        addPrivateFunction(EMPTY_STREAM, ABooleanTypeComputer.INSTANCE, true);

        addFunction(FLOAT_CONSTRUCTOR, AFloatTypeComputer.INSTANCE, true);
        addPrivateFunction(FUZZY_EQ, BooleanFunctionTypeComputer.INSTANCE, true);
        addPrivateFunction(GET_HANDLE, null, true);
        addPrivateFunction(GET_ITEM, NonTaggedGetItemResultType.INSTANCE, true);
        addPrivateFunction(GET_DATA, null, true);
        addPrivateFunction(GRAM_TOKENS, OrderedListOfAStringTypeComputer.INSTANCE, true);
        addPrivateFunction(HASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(HASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE, true);
        addPrivateFunction(INDEX_SEARCH, AnyTypeComputer.INSTANCE, true);
        addFunction(INT8_CONSTRUCTOR, AInt8TypeComputer.INSTANCE, true);
        addFunction(INT16_CONSTRUCTOR, AInt16TypeComputer.INSTANCE, true);
        addFunction(INT32_CONSTRUCTOR, AInt32TypeComputer.INSTANCE, true);
        addFunction(INT64_CONSTRUCTOR, AInt64TypeComputer.INSTANCE, true);
        addFunction(LEN, AInt64TypeComputer.INSTANCE, true);
        addFunction(LINE_CONSTRUCTOR, ALineTypeComputer.INSTANCE, true);
        addPrivateFunction(LISTIFY, OrderedListConstructorTypeComputer.INSTANCE, true);
        addPrivateFunction(MAKE_FIELD_INDEX_HANDLE, null, true);
        addPrivateFunction(MAKE_FIELD_NAME_HANDLE, null, true);
        addFunction(NULL_CONSTRUCTOR, AMissingTypeComputer.INSTANCE, true);

        addPrivateFunction(NUMERIC_UNARY_MINUS, UnaryMinusTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_SUBTRACT, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_MULTIPLY, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_DIVIDE, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_MOD, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_IDIV, AInt64TypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ABS, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ACOS, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ASIN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ATAN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ATAN2, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_COS, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SIN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_TAN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_EXP, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_LN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_LOG, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SQRT, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SIGN, NumericInt8OutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_CEILING, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_FLOOR, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN2, NumericRound2TypeComputer.INSTANCE, true);
        addFunction(NUMERIC_TRUNC, NumericRound2TypeComputer.INSTANCE, true);

        addFunction(BINARY_LENGTH, UnaryBinaryInt64TypeComputer.INSTANCE, true);
        addFunction(PARSE_BINARY, ABinaryTypeComputer.INSTANCE, true);
        addFunction(PRINT_BINARY, AStringTypeComputer.INSTANCE, true);
        addFunction(BINARY_CONCAT, ABinaryTypeComputer.INSTANCE, true);
        addFunction(SUBBINARY_FROM, ABinaryTypeComputer.INSTANCE, true);
        addFunction(SUBBINARY_FROM_TO, ABinaryTypeComputer.INSTANCE, true);
        addFunction(FIND_BINARY, AInt64TypeComputer.INSTANCE, true);
        addFunction(FIND_BINARY_FROM, AInt64TypeComputer.INSTANCE, true);

        addFunction(STRING_LIKE, BooleanFunctionTypeComputer.INSTANCE, true);
        addFunction(STRING_CONTAINS, ABooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_TO_CODEPOINT, StringToInt64ListTypeComputer.INSTANCE, true);
        addFunction(CODEPOINT_TO_STRING, AStringTypeComputer.INSTANCE, true);
        addFunction(STRING_CONCAT, AStringTypeComputer.INSTANCE, true);
        addFunction(SUBSTRING2, StringIntToStringTypeComputer.INSTANCE, true);
        addFunction(STRING_LENGTH, UnaryStringInt64TypeComputer.INSTANCE, true);
        addFunction(STRING_LOWERCASE, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_UPPERCASE, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_INITCAP, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_TRIM, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_LTRIM, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_RTRIM, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_TRIM2, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_LTRIM2, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_RTRIM2, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_POSITION, StringInt32TypeComputer.INSTANCE, true);
        addFunction(STRING_STARTS_WITH, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_ENDS_WITH, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_MATCHES, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_MATCHES_WITH_FLAG, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_REGEXP_LIKE, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_REGEXP_LIKE_WITH_FLAG, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_REGEXP_POSITION, StringInt32TypeComputer.INSTANCE, true);
        addFunction(STRING_REGEXP_POSITION_WITH_FLAG, StringInt32TypeComputer.INSTANCE, true);
        addFunction(STRING_REPLACE, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_REPLACE_WITH_FLAG, StringStringTypeComputer.INSTANCE, true);
        addFunction(SUBSTRING_BEFORE, StringStringTypeComputer.INSTANCE, true);
        addFunction(SUBSTRING_AFTER, StringStringTypeComputer.INSTANCE, true);
        addPrivateFunction(STRING_EQUAL, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_JOIN, AStringTypeComputer.INSTANCE, true);
        addFunction(STRING_REPEAT, StringIntToStringTypeComputer.INSTANCE, true);
        addFunction(STRING_SPLIT, StringToStringListTypeComputer.INSTANCE, true);

        addPrivateFunction(ORDERED_LIST_CONSTRUCTOR, OrderedListConstructorTypeComputer.INSTANCE, true);
        addFunction(POINT_CONSTRUCTOR, APointTypeComputer.INSTANCE, true);
        addFunction(POINT3D_CONSTRUCTOR, APoint3DTypeComputer.INSTANCE, true);
        addFunction(POLYGON_CONSTRUCTOR, APolygonTypeComputer.INSTANCE, true);
        addPrivateFunction(PREFIX_LEN_JACCARD, AInt32TypeComputer.INSTANCE, true);
        addFunction(RANGE, AInt64TypeComputer.INSTANCE, true);
        addFunction(RECTANGLE_CONSTRUCTOR, ARectangleTypeComputer.INSTANCE, true);

        // Aggregate Functions
        addFunction(MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(NON_EMPTY_STREAM, ABooleanTypeComputer.INSTANCE, true);
        addFunction(COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SUM, NumericAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SUM, NumericAggTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SCALAR_FIRST_ELEMENT, CollectionMemberResultType.INSTANCE, true);
        addPrivateFunction(FIRST_ELEMENT, PropagateTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_FIRST_ELEMENT, PropagateTypeComputer.INSTANCE, true);

        addPrivateFunction(SERIAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SQL_SUM, NumericAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SQL_SUM, NumericAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(SCALAR_GLOBAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SCALAR_LOCAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_MAX, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SCALAR_MIN, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SCALAR_SUM, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_AVG, LocalAvgTypeComputer.INSTANCE, true);

        addFunction(SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(GLOBAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addFunction(SQL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addFunction(SQL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_MAX, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SQL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_MIN, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SQL_SUM, NumericAggTypeComputer.INSTANCE, true);
        addPrivateFunction(LOCAL_SQL_SUM, NumericAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(SCALAR_GLOBAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SCALAR_LOCAL_SQL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_MAX, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SCALAR_SQL_MIN, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SCALAR_SQL_SUM, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addPrivateFunction(INTERMEDIATE_SQL_AVG, LocalAvgTypeComputer.INSTANCE, true);

        addPrivateFunction(SERIAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_COUNT, AInt64TypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_GLOBAL_AVG, NullableDoubleTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_INTERMEDIATE_AVG, LocalAvgTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_SUM, NumericAggTypeComputer.INSTANCE, true);
        addPrivateFunction(SERIAL_LOCAL_SUM, NumericAggTypeComputer.INSTANCE, true);

        // Similarity functions
        addFunction(EDIT_DISTANCE_CONTAINS, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addFunction(SIMILARITY_JACCARD, AFloatTypeComputer.INSTANCE, true);
        addFunction(SIMILARITY_JACCARD_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_SORTED, AFloatTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_SORTED_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_PREFIX, AFloatTypeComputer.INSTANCE, true);
        addPrivateFunction(SIMILARITY_JACCARD_PREFIX_CHECK, OrderedListOfAnyTypeComputer.INSTANCE, true);

        // Spatial functions
        addFunction(SPATIAL_AREA, ADoubleTypeComputer.INSTANCE, true);
        addFunction(SPATIAL_CELL, ARectangleTypeComputer.INSTANCE, true);
        addFunction(SPATIAL_DISTANCE, ADoubleTypeComputer.INSTANCE, true);
        addFunctionWithDomain(SPATIAL_INTERSECT, ATypeHierarchy.Domain.SPATIAL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(GET_POINT_X_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_POINT_Y_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_CIRCLE_RADIUS_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_CIRCLE_CENTER_ACCESSOR, APointTypeComputer.INSTANCE, true);
        addFunction(GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR, OrderedListOfAPointTypeComputer.INSTANCE, true);
        addFunction(STRING_CONSTRUCTOR, AStringTypeComputer.INSTANCE, true);

        // Binary functions
        addFunction(BINARY_HEX_CONSTRUCTOR, ABinaryTypeComputer.INSTANCE, true);
        addFunction(BINARY_BASE64_CONSTRUCTOR, ABinaryTypeComputer.INSTANCE, true);

        addPrivateFunction(SUBSET_COLLECTION, SubsetCollectionTypeComputer.INSTANCE, true);
        addFunction(SUBSTRING, SubstringTypeComputer.INSTANCE, true);
        addFunction(SWITCH_CASE, SwitchCaseComputer.INSTANCE, true);
        addPrivateFunction(INJECT_FAILURE, InjectFailureTypeComputer.INSTANCE, true);
        addPrivateFunction(CAST_TYPE, CastTypeComputer.INSTANCE, true);

        addFunction(TID, AInt64TypeComputer.INSTANCE, true);
        addFunction(TIME_CONSTRUCTOR, ATimeTypeComputer.INSTANCE, true);
        addPrivateFunction(TYPE_OF, null, true);
        addPrivateFunction(UNORDERED_LIST_CONSTRUCTOR, UnorderedListConstructorTypeComputer.INSTANCE, true);
        addFunction(WORD_TOKENS, OrderedListOfAStringTypeComputer.INSTANCE, true);

        // objects
        addFunction(RECORD_MERGE, RecordMergeTypeComputer.INSTANCE, true);
        addFunction(ADD_FIELDS, RecordAddFieldsTypeComputer.INSTANCE, true);
        addFunction(REMOVE_FIELDS, RecordRemoveFieldsTypeComputer.INSTANCE, true);
        addPrivateFunction(CLOSED_RECORD_CONSTRUCTOR, ClosedRecordConstructorResultType.INSTANCE, true);
        addPrivateFunction(OPEN_RECORD_CONSTRUCTOR, OpenRecordConstructorResultType.INSTANCE, true);
        addPrivateFunction(FIELD_ACCESS_BY_INDEX, FieldAccessByIndexResultType.INSTANCE, true);
        addPrivateFunction(FIELD_ACCESS_NESTED, FieldAccessNestedResultType.INSTANCE, true);
        addPrivateFunction(FIELD_ACCESS_BY_NAME, FieldAccessByNameResultType.INSTANCE, true);
        addFunction(GET_RECORD_FIELDS, OrderedListOfAnyTypeComputer.INSTANCE, true);
        addFunction(GET_RECORD_FIELD_VALUE, FieldAccessNestedResultType.INSTANCE, true);
        addFunction(RECORD_PAIRS, RecordPairsTypeComputer.INSTANCE, true);

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
        addFunction(UNIX_TIME_FROM_TIME_IN_MS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_DATETIME_IN_MS, AInt64TypeComputer.INSTANCE, true);
        addFunction(UNIX_TIME_FROM_DATETIME_IN_SECS, AInt64TypeComputer.INSTANCE, true);
        addFunction(DATE_FROM_UNIX_TIME_IN_DAYS, ADateTypeComputer.INSTANCE, true);
        addFunction(DATE_FROM_DATETIME, ADateTypeComputer.INSTANCE, true);
        addFunction(TIME_FROM_UNIX_TIME_IN_MS, ATimeTypeComputer.INSTANCE, true);
        addFunction(TIME_FROM_DATETIME, ATimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_DATE_TIME, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_MS, ADateTimeTypeComputer.INSTANCE, true);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_SECS, ADateTimeTypeComputer.INSTANCE, true);
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
        addFunction(CURRENT_TIME, ATimeTypeComputer.INSTANCE, false);
        addFunction(CURRENT_DATETIME, ADateTimeTypeComputer.INSTANCE, false);
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
        addFunction(RESOLVE, AnyTypeComputer.INSTANCE, false);

        addPrivateFunction(COLLECTION_TO_SEQUENCE, CollectionToSequenceTypeComputer.INSTANCE, true);

        // external lookup
        addPrivateFunction(EXTERNAL_LOOKUP, AnyTypeComputer.INSTANCE, false);

        // unnesting function
        addPrivateFunction(SCAN_COLLECTION, CollectionMemberResultType.INSTANCE, true);

        String metadataFunctionLoaderClassName = "org.apache.asterix.metadata.functions.MetadataBuiltinFunctions";
        try {
            Class.forName(metadataFunctionLoaderClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    static {
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_AVG), getAsterixFunctionInfo(AVG));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_COUNT), getAsterixFunctionInfo(COUNT));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_GLOBAL_AVG), getAsterixFunctionInfo(GLOBAL_AVG));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_LOCAL_AVG), getAsterixFunctionInfo(LOCAL_AVG));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_MAX), getAsterixFunctionInfo(MAX));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_MIN), getAsterixFunctionInfo(MIN));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_SUM), getAsterixFunctionInfo(SUM));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_FIRST_ELEMENT),
                getAsterixFunctionInfo(FIRST_ELEMENT));
        // SQL Aggregate Functions
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_SQL_AVG), getAsterixFunctionInfo(SQL_AVG));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_SQL_COUNT), getAsterixFunctionInfo(SQL_COUNT));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_GLOBAL_SQL_AVG),
                getAsterixFunctionInfo(GLOBAL_SQL_AVG));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_LOCAL_SQL_AVG),
                getAsterixFunctionInfo(LOCAL_SQL_AVG));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_SQL_MAX), getAsterixFunctionInfo(SQL_MAX));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_SQL_MIN), getAsterixFunctionInfo(SQL_MIN));
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(SCALAR_SQL_SUM), getAsterixFunctionInfo(SQL_SUM));
    }

    static {
        addAgg(AVG);
        addAgg(LOCAL_AVG);
        addAgg(GLOBAL_AVG);
        addLocalAgg(AVG, LOCAL_AVG);
        addIntermediateAgg(AVG, INTERMEDIATE_AVG);
        addIntermediateAgg(LOCAL_AVG, INTERMEDIATE_AVG);
        addIntermediateAgg(GLOBAL_AVG, INTERMEDIATE_AVG);
        addGlobalAgg(AVG, GLOBAL_AVG);

        addAgg(COUNT);
        addLocalAgg(COUNT, COUNT);
        addIntermediateAgg(COUNT, SUM);
        addGlobalAgg(COUNT, SUM);

        addAgg(MAX);
        addAgg(LOCAL_MAX);
        addLocalAgg(MAX, LOCAL_MAX);
        addIntermediateAgg(LOCAL_MAX, MAX);
        addIntermediateAgg(MAX, MAX);
        addGlobalAgg(MAX, MAX);

        addAgg(SCALAR_FIRST_ELEMENT);
        addAgg(LOCAL_FIRST_ELEMENT);
        addLocalAgg(FIRST_ELEMENT, LOCAL_FIRST_ELEMENT);
        addIntermediateAgg(LOCAL_FIRST_ELEMENT, FIRST_ELEMENT);
        addIntermediateAgg(FIRST_ELEMENT, FIRST_ELEMENT);
        addGlobalAgg(FIRST_ELEMENT, FIRST_ELEMENT);

        addAgg(MIN);
        addLocalAgg(MIN, LOCAL_MIN);
        addIntermediateAgg(LOCAL_MIN, MIN);
        addIntermediateAgg(MIN, MIN);
        addGlobalAgg(MIN, MIN);

        addAgg(SUM);
        addAgg(LOCAL_SUM);
        addLocalAgg(SUM, LOCAL_SUM);
        addIntermediateAgg(LOCAL_SUM, SUM);
        addIntermediateAgg(SUM, SUM);
        addGlobalAgg(SUM, SUM);

        addAgg(LISTIFY);

        // serializable aggregate functions
        addSerialAgg(AVG, SERIAL_AVG);
        addSerialAgg(COUNT, SERIAL_COUNT);
        addSerialAgg(SUM, SERIAL_SUM);
        addSerialAgg(LOCAL_SUM, SERIAL_LOCAL_SUM);
        addSerialAgg(LOCAL_AVG, SERIAL_LOCAL_AVG);
        addSerialAgg(GLOBAL_AVG, SERIAL_GLOBAL_AVG);

        addAgg(SERIAL_COUNT);
        addLocalAgg(SERIAL_COUNT, SERIAL_COUNT);
        addIntermediateAgg(SERIAL_COUNT, SERIAL_SUM);
        addGlobalAgg(SERIAL_COUNT, SERIAL_SUM);

        addAgg(SERIAL_AVG);
        addAgg(SERIAL_LOCAL_AVG);
        addAgg(SERIAL_GLOBAL_AVG);
        addLocalAgg(SERIAL_AVG, SERIAL_LOCAL_AVG);
        addIntermediateAgg(SERIAL_AVG, SERIAL_INTERMEDIATE_AVG);
        addIntermediateAgg(SERIAL_LOCAL_AVG, SERIAL_INTERMEDIATE_AVG);
        addIntermediateAgg(SERIAL_GLOBAL_AVG, SERIAL_INTERMEDIATE_AVG);
        addGlobalAgg(SERIAL_AVG, SERIAL_GLOBAL_AVG);

        addAgg(SERIAL_SUM);
        addAgg(SERIAL_LOCAL_SUM);
        addLocalAgg(SERIAL_SUM, SERIAL_LOCAL_SUM);
        addIntermediateAgg(SERIAL_SUM, SERIAL_SUM);
        addIntermediateAgg(SERIAL_LOCAL_SUM, SERIAL_SUM);
        addGlobalAgg(SERIAL_SUM, SERIAL_SUM);

        // SQL Aggregate Functions
        addAgg(SQL_AVG);
        addAgg(LOCAL_SQL_AVG);
        addAgg(GLOBAL_SQL_AVG);
        addLocalAgg(SQL_AVG, LOCAL_SQL_AVG);
        addIntermediateAgg(SQL_AVG, INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(LOCAL_SQL_AVG, INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(GLOBAL_SQL_AVG, INTERMEDIATE_SQL_AVG);
        addGlobalAgg(SQL_AVG, GLOBAL_SQL_AVG);

        addAgg(SQL_COUNT);
        addLocalAgg(SQL_COUNT, SQL_COUNT);
        addIntermediateAgg(SQL_COUNT, SQL_SUM);
        addGlobalAgg(SQL_COUNT, SQL_SUM);

        addAgg(SQL_MAX);
        addAgg(LOCAL_SQL_MAX);
        addLocalAgg(SQL_MAX, LOCAL_SQL_MAX);
        addIntermediateAgg(LOCAL_SQL_MAX, SQL_MAX);
        addIntermediateAgg(SQL_MAX, SQL_MAX);
        addGlobalAgg(SQL_MAX, SQL_MAX);

        addAgg(SQL_MIN);
        addLocalAgg(SQL_MIN, LOCAL_SQL_MIN);
        addIntermediateAgg(LOCAL_SQL_MIN, SQL_MIN);
        addIntermediateAgg(SQL_MIN, SQL_MIN);
        addGlobalAgg(SQL_MIN, SQL_MIN);

        addAgg(SQL_SUM);
        addAgg(LOCAL_SQL_SUM);
        addLocalAgg(SQL_SUM, LOCAL_SQL_SUM);
        addIntermediateAgg(LOCAL_SQL_SUM, SQL_SUM);
        addIntermediateAgg(SQL_SUM, SQL_SUM);
        addGlobalAgg(SQL_SUM, SQL_SUM);

        // SQL serializable aggregate functions
        addSerialAgg(SQL_AVG, SERIAL_SQL_AVG);
        addSerialAgg(SQL_COUNT, SERIAL_SQL_COUNT);
        addSerialAgg(SQL_SUM, SERIAL_SQL_SUM);
        addSerialAgg(LOCAL_SQL_SUM, SERIAL_LOCAL_SQL_SUM);
        addSerialAgg(LOCAL_SQL_AVG, SERIAL_LOCAL_SQL_AVG);
        addSerialAgg(GLOBAL_SQL_AVG, SERIAL_GLOBAL_SQL_AVG);

        addAgg(SERIAL_SQL_COUNT);
        addLocalAgg(SERIAL_SQL_COUNT, SERIAL_SQL_COUNT);
        addIntermediateAgg(SERIAL_SQL_COUNT, SERIAL_SQL_SUM);
        addGlobalAgg(SERIAL_SQL_COUNT, SERIAL_SQL_SUM);

        addAgg(SERIAL_SQL_AVG);
        addAgg(SERIAL_LOCAL_SQL_AVG);
        addAgg(SERIAL_GLOBAL_SQL_AVG);
        addLocalAgg(SERIAL_SQL_AVG, SERIAL_LOCAL_SQL_AVG);
        addIntermediateAgg(SERIAL_SQL_AVG, SERIAL_INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(SERIAL_LOCAL_SQL_AVG, SERIAL_INTERMEDIATE_SQL_AVG);
        addIntermediateAgg(SERIAL_GLOBAL_SQL_AVG, SERIAL_INTERMEDIATE_SQL_AVG);
        addGlobalAgg(SERIAL_SQL_AVG, SERIAL_GLOBAL_SQL_AVG);

        addAgg(SERIAL_SQL_SUM);
        addAgg(SERIAL_LOCAL_SQL_SUM);
        addLocalAgg(SERIAL_SQL_SUM, SERIAL_LOCAL_SQL_SUM);
        addIntermediateAgg(SERIAL_LOCAL_SQL_SUM, SERIAL_SQL_SUM);
        addIntermediateAgg(SERIAL_SQL_SUM, SERIAL_SQL_SUM);
        addGlobalAgg(SERIAL_SQL_SUM, SERIAL_SQL_SUM);

    }

    static {
        datasetFunctions.add(getAsterixFunctionInfo(DATASET));
        datasetFunctions.add(getAsterixFunctionInfo(FEED_COLLECT));
        datasetFunctions.add(getAsterixFunctionInfo(FEED_INTERCEPT));
        datasetFunctions.add(getAsterixFunctionInfo(INDEX_SEARCH));
    }

    static {
        addUnnestFun(DATASET, false);
        addUnnestFun(FEED_COLLECT, false);
        addUnnestFun(FEED_INTERCEPT, false);
        addUnnestFun(RANGE, true);
        addUnnestFun(SCAN_COLLECTION, false);
        addUnnestFun(SUBSET_COLLECTION, false);
    }

    public static void addDatasetFunction(FunctionIdentifier fi) {
        datasetFunctions.add(getAsterixFunctionInfo(fi));
    }

    public static boolean isDatasetFunction(FunctionIdentifier fi) {
        return datasetFunctions.contains(getAsterixFunctionInfo(fi));
    }

    public static boolean isBuiltinCompilerFunction(FunctionSignature signature, boolean includePrivateFunctions) {
        FunctionIdentifier fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, signature.getName(),
                signature.getArity());
        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        if (builtinPublicFunctionsSet.keySet().contains(finfo)
                || (includePrivateFunctions && builtinPrivateFunctionsSet.keySet().contains(finfo))) {
            return true;
        }
        fi = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, signature.getName(),
                signature.getArity());
        finfo = getAsterixFunctionInfo(fi);
        if (builtinPublicFunctionsSet.keySet().contains(finfo)
                || (includePrivateFunctions && builtinPrivateFunctionsSet.keySet().contains(finfo))) {
            return true;
        }

        return false;
    }

    public static boolean isBuiltinAggregateFunction(FunctionIdentifier fi) {
        return builtinAggregateFunctions.contains(getAsterixFunctionInfo(fi));
    }

    public static boolean isBuiltinUnnestingFunction(FunctionIdentifier fi) {
        return builtinUnnestingFunctions.get(getAsterixFunctionInfo(fi)) != null;
    }

    public static boolean returnsUniqueValues(FunctionIdentifier fi) {
        Boolean ruv = builtinUnnestingFunctions.get(getAsterixFunctionInfo(fi));
        return ruv != null && ruv.booleanValue();
    }

    public static FunctionIdentifier getLocalAggregateFunction(FunctionIdentifier fi) {
        return aggregateToLocalAggregate.get(getAsterixFunctionInfo(fi)).getFunctionIdentifier();
    }

    public static FunctionIdentifier getGlobalAggregateFunction(FunctionIdentifier fi) {
        return aggregateToGlobalAggregate.get(getAsterixFunctionInfo(fi)).getFunctionIdentifier();
    }

    public static FunctionIdentifier getIntermediateAggregateFunction(FunctionIdentifier fi) {
        IFunctionInfo funcInfo = aggregateToIntermediateAggregate.get(getAsterixFunctionInfo(fi));
        if (funcInfo == null) {
            return null;
        }
        return funcInfo.getFunctionIdentifier();
    }

    public static FunctionIdentifier getBuiltinFunctionIdentifier(FunctionIdentifier fi) {
        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        return finfo == null ? null : finfo.getFunctionIdentifier();
    }

    public static AggregateFunctionCallExpression makeAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {
        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        IFunctionInfo fiLocal = aggregateToLocalAggregate.get(finfo);
        IFunctionInfo fiGlobal = aggregateToGlobalAggregate.get(finfo);

        if (fiLocal != null && fiGlobal != null) {
            AggregateFunctionCallExpression fun = new AggregateFunctionCallExpression(finfo, true, args);
            fun.setStepTwoAggregate(fiGlobal);
            fun.setStepOneAggregate(fiLocal);
            return fun;
        } else {
            return new AggregateFunctionCallExpression(finfo, false, args);
        }
    }

    public static boolean isAggregateFunctionSerializable(FunctionIdentifier fi) {
        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        return aggregateToSerializableAggregate.get(finfo) != null;
    }

    public static AggregateFunctionCallExpression makeSerializableAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {

        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        IFunctionInfo serializableFinfo = aggregateToSerializableAggregate.get(finfo);
        if (serializableFinfo == null) {
            throw new IllegalStateException(
                    "no serializable implementation for aggregate function " + serializableFinfo);
        }

        IFunctionInfo fiLocal = aggregateToLocalAggregate.get(serializableFinfo);
        IFunctionInfo fiGlobal = aggregateToGlobalAggregate.get(serializableFinfo);

        if (fiLocal != null && fiGlobal != null) {
            AggregateFunctionCallExpression fun = new AggregateFunctionCallExpression(serializableFinfo, true, args);
            fun.setStepTwoAggregate(fiGlobal);
            fun.setStepOneAggregate(fiLocal);
            return fun;
        } else {
            return new AggregateFunctionCallExpression(serializableFinfo, false, args);
        }
    }

    public static IResultTypeComputer getResultTypeComputer(FunctionIdentifier fi) {
        return funTypeComputer.get(getAsterixFunctionInfo(fi));
    }

    public static FunctionIdentifier getAggregateFunction(FunctionIdentifier scalarVersionOfAggregate) {
        IFunctionInfo finfo = scalarToAggregateFunctionMap.get(getAsterixFunctionInfo(scalarVersionOfAggregate));
        return finfo == null ? null : finfo.getFunctionIdentifier();
    }

    public static void addFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer, boolean isFunctional) {
        addFunctionWithDomain(fi, ATypeHierarchy.Domain.ANY, typeComputer, isFunctional);
    }

    public static void addFunctionWithDomain(FunctionIdentifier fi, ATypeHierarchy.Domain funcDomain,
            IResultTypeComputer typeComputer, boolean isFunctional) {
        IFunctionInfo functionInfo = new AsterixFunctionInfo(fi, isFunctional);
        builtinPublicFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        registeredFunctions.put(fi, functionInfo);
        registeredFunctionsDomain.put(functionInfo, funcDomain);
    }

    public static void addPrivateFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer,
            boolean isFunctional) {
        IFunctionInfo functionInfo = new AsterixFunctionInfo(fi, isFunctional);
        builtinPrivateFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        registeredFunctions.put(fi, functionInfo);
    }

    private static void addAgg(FunctionIdentifier fi) {
        builtinAggregateFunctions.add(getAsterixFunctionInfo(fi));
    }

    private static void addLocalAgg(FunctionIdentifier fi, FunctionIdentifier localfi) {
        aggregateToLocalAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(localfi));
    }

    private static void addIntermediateAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToIntermediateAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(globalfi));
    }

    private static void addGlobalAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToGlobalAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(globalfi));
        globalAggregateFunctions.add(getAsterixFunctionInfo(globalfi));
    }

    public static void addUnnestFun(FunctionIdentifier fi, boolean returnsUniqueValues) {
        builtinUnnestingFunctions.put(getAsterixFunctionInfo(fi), returnsUniqueValues);
    }

    private static void addSerialAgg(FunctionIdentifier fi, FunctionIdentifier serialfi) {
        aggregateToSerializableAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(serialfi));
    }

    static {
        spatialFilterFunctions.put(getAsterixFunctionInfo(AsterixBuiltinFunctions.SPATIAL_INTERSECT),
                SpatialFilterKind.SI);
    }

    public static boolean isGlobalAggregateFunction(FunctionIdentifier fi) {
        return globalAggregateFunctions.contains(getAsterixFunctionInfo(fi));
    }

    public static boolean isSpatialFilterFunction(FunctionIdentifier fi) {
        return spatialFilterFunctions.get(getAsterixFunctionInfo(fi)) != null;
    }

    static {
        similarityFunctions.add(getAsterixFunctionInfo(SIMILARITY_JACCARD));
        similarityFunctions.add(getAsterixFunctionInfo(SIMILARITY_JACCARD_CHECK));
        similarityFunctions.add(getAsterixFunctionInfo(EDIT_DISTANCE));
        similarityFunctions.add(getAsterixFunctionInfo(EDIT_DISTANCE_CHECK));
        similarityFunctions.add(getAsterixFunctionInfo(EDIT_DISTANCE_CONTAINS));
    }

    public static boolean isSimilarityFunction(FunctionIdentifier fi) {
        return similarityFunctions.contains(getAsterixFunctionInfo(fi));
    }

}
