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
import org.apache.asterix.common.functions.FunctionSignature;
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
import org.apache.asterix.om.typecomputer.impl.LocalAvgTypeComputer;
import org.apache.asterix.om.typecomputer.impl.LocalSingleVarStatisticsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.MinMaxAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.MissingIfTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NonTaggedGetItemResultType;
import org.apache.asterix.om.typecomputer.impl.NotUnknownTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NullIfTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NullableDoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericAddSubMulDivTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericDivideTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericDoubleOutputFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericInt8OutputFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericRound2TypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericRoundFunctionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.NumericSumAggTypeComputer;
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
import org.apache.asterix.om.typecomputer.impl.RecordRemoveFieldsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ScalarArrayAggTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ScalarVersionOfAggregateResultType;
import org.apache.asterix.om.typecomputer.impl.SleepTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringBooleanTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringInt32TypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringJoinTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringStringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringToInt64ListTypeComputer;
import org.apache.asterix.om.typecomputer.impl.StringToStringListTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SubsetCollectionTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SubstringTypeComputer;
import org.apache.asterix.om.typecomputer.impl.SwitchCaseComputer;
import org.apache.asterix.om.typecomputer.impl.ToArrayTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToBigIntTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToDoubleTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToNumberTypeComputer;
import org.apache.asterix.om.typecomputer.impl.ToObjectTypeComputer;
import org.apache.asterix.om.typecomputer.impl.TreatAsTypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryBinaryInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryMinusTypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnaryStringInt64TypeComputer;
import org.apache.asterix.om.typecomputer.impl.UnorderedListConstructorTypeComputer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
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

    public enum SpatialFilterKind {
        SI
    }

    private static final FunctionInfoRepository registeredFunctions = new FunctionInfoRepository();
    private static final Map<IFunctionInfo, ATypeHierarchy.Domain> registeredFunctionsDomain = new HashMap<>();

    // it is supposed to be an identity mapping
    private static final Map<IFunctionInfo, IFunctionInfo> builtinPublicFunctionsSet = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> builtinPrivateFunctionsSet = new HashMap<>();
    private static final Map<IFunctionInfo, IResultTypeComputer> funTypeComputer = new HashMap<>();
    private static final Map<IFunctionInfo, Set<? extends BuiltinFunctionProperty>> builtinFunctionProperties =
            new HashMap<>();
    private static final Set<IFunctionInfo> builtinAggregateFunctions = new HashSet<>();
    private static final Map<IFunctionInfo, IFunctionToDataSourceRewriter> datasourceFunctions = new HashMap<>();
    private static final Set<IFunctionInfo> similarityFunctions = new HashSet<>();
    private static final Set<IFunctionInfo> globalAggregateFunctions = new HashSet<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToLocalAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToIntermediateAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToGlobalAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToSerializableAggregate = new HashMap<>();
    private static final Map<IFunctionInfo, Boolean> builtinUnnestingFunctions = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> scalarToAggregateFunctionMap = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> distinctToRegularAggregateFunctionMap = new HashMap<>();
    private static final Map<IFunctionInfo, IFunctionInfo> sqlToWindowFunctions = new HashMap<>();
    private static final Set<IFunctionInfo> windowFunctions = new HashSet<>();

    private static final Map<IFunctionInfo, SpatialFilterKind> spatialFilterFunctions = new HashMap<>();

    public static final FunctionIdentifier TYPE_OF = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "type-of", 1);
    public static final FunctionIdentifier GET_HANDLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-handle", 2);
    public static final FunctionIdentifier GET_DATA =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-data", 2);
    public static final FunctionIdentifier GET_ITEM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-item", 2);
    public static final FunctionIdentifier ANY_COLLECTION_MEMBER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "any-collection-member", 1);
    public static final FunctionIdentifier LEN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "len", 1);
    public static final FunctionIdentifier CONCAT_NON_NULL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "concat-non-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier EMPTY_STREAM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "empty-stream", 0);
    public static final FunctionIdentifier NON_EMPTY_STREAM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "non-empty-stream", 0);
    public static final FunctionIdentifier ORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "ordered-list-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier UNORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unordered-list-constructor", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier DEEP_EQUAL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "deep-equal", 2);

    // array functions
    public static final FunctionIdentifier ARRAY_REMOVE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-remove", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_PUT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-put", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_PREPEND =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-prepend", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_APPEND =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-append", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_INSERT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-insert", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_POSITION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-position", 2);
    public static final FunctionIdentifier ARRAY_REPEAT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-repeat", 2);
    public static final FunctionIdentifier ARRAY_REVERSE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-reverse", 1);
    public static final FunctionIdentifier ARRAY_CONTAINS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-contains", 2);
    public static final FunctionIdentifier ARRAY_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-distinct", 1);
    public static final FunctionIdentifier ARRAY_SORT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-sort", 1);
    public static final FunctionIdentifier ARRAY_UNION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-union", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_INTERSECT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-intersect", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_IFNULL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-ifnull", 1);
    public static final FunctionIdentifier ARRAY_CONCAT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-concat", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_RANGE_WITHOUT_STEP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-range", 2);
    public static final FunctionIdentifier ARRAY_RANGE_WITH_STEP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-range", 3);
    public static final FunctionIdentifier ARRAY_FLATTEN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-flatten", 2);
    public static final FunctionIdentifier ARRAY_REPLACE_WITHOUT_MAXIMUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-replace", 3);
    public static final FunctionIdentifier ARRAY_REPLACE_WITH_MAXIMUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-replace", 4);
    public static final FunctionIdentifier ARRAY_SYMDIFF =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-symdiff", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_SYMDIFFN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-symdiffn", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier ARRAY_STAR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-star", 1);
    public static final FunctionIdentifier ARRAY_SLICE_WITHOUT_END_POSITION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-slice", 2);
    public static final FunctionIdentifier ARRAY_SLICE_WITH_END_POSITION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "array-slice", 3);

    // objects
    public static final FunctionIdentifier RECORD_MERGE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-merge", 2);
    public static final FunctionIdentifier RECORD_MERGE_IGNORE_DUPLICATES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-merge-ignore-duplicates", 2);
    public static final FunctionIdentifier RECORD_CONCAT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-concat", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RECORD_CONCAT_STRICT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-concat-strict", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier REMOVE_FIELDS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-remove-fields", 2);
    public static final FunctionIdentifier ADD_FIELDS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-add-fields", 2);

    public static final FunctionIdentifier CLOSED_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "closed-object-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier OPEN_RECORD_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "open-object-constructor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier FIELD_ACCESS_BY_INDEX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "field-access-by-index", 2);
    public static final FunctionIdentifier FIELD_ACCESS_BY_NAME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "field-access-by-name", 2);
    public static final FunctionIdentifier FIELD_ACCESS_NESTED =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "field-access-nested", 2);
    public static final FunctionIdentifier GET_RECORD_FIELDS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-object-fields", 1);
    public static final FunctionIdentifier GET_RECORD_FIELD_VALUE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-object-field-value", 2);
    public static final FunctionIdentifier RECORD_LENGTH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-length", 1);
    public static final FunctionIdentifier RECORD_NAMES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-names", 1);
    public static final FunctionIdentifier RECORD_PAIRS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-pairs", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier GEOMETRY_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-geom-from-geojson", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RECORD_REMOVE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-remove", 2);
    public static final FunctionIdentifier RECORD_RENAME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-rename", 3);
    public static final FunctionIdentifier RECORD_UNWRAP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-unwrap", 1);
    public static final FunctionIdentifier RECORD_REPLACE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-replace", 3);
    public static final FunctionIdentifier RECORD_ADD =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-add", 3);
    public static final FunctionIdentifier RECORD_PUT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-put", 3);
    public static final FunctionIdentifier RECORD_VALUES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "object-values", 1);
    public static final FunctionIdentifier PAIRS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "pairs", 1);

    // numeric
    public static final FunctionIdentifier NUMERIC_UNARY_MINUS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "numeric-unary-minus", 1);
    public static final FunctionIdentifier NUMERIC_SUBTRACT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "numeric-subtract", 2);
    public static final FunctionIdentifier NUMERIC_MULTIPLY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "numeric-multiply", 2);
    public static final FunctionIdentifier NUMERIC_DIVIDE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "numeric-divide", 2);
    public static final FunctionIdentifier NUMERIC_MOD =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "numeric-mod", 2);
    public static final FunctionIdentifier NUMERIC_DIV =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "numeric-div", 2);
    public static final FunctionIdentifier NUMERIC_POWER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "power", 2);
    public static final FunctionIdentifier NUMERIC_ABS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "abs", 1);
    public static final FunctionIdentifier NUMERIC_ACOS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "acos", 1);
    public static final FunctionIdentifier NUMERIC_ASIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "asin", 1);
    public static final FunctionIdentifier NUMERIC_ATAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "atan", 1);
    public static final FunctionIdentifier NUMERIC_ATAN2 =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "atan2", 2);
    public static final FunctionIdentifier NUMERIC_DEGREES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "degrees", 1);
    public static final FunctionIdentifier NUMERIC_RADIANS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "radians", 1);
    public static final FunctionIdentifier NUMERIC_COS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cos", 1);
    public static final FunctionIdentifier NUMERIC_COSH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cosh", 1);
    public static final FunctionIdentifier NUMERIC_SIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sin", 1);
    public static final FunctionIdentifier NUMERIC_SINH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sinh", 1);
    public static final FunctionIdentifier NUMERIC_TAN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tan", 1);
    public static final FunctionIdentifier NUMERIC_TANH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tanh", 1);
    public static final FunctionIdentifier NUMERIC_EXP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "exp", 1);
    public static final FunctionIdentifier NUMERIC_LN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ln", 1);
    public static final FunctionIdentifier NUMERIC_LOG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "log", 1);
    public static final FunctionIdentifier NUMERIC_SQRT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sqrt", 1);
    public static final FunctionIdentifier NUMERIC_SIGN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sign", 1);
    public static final FunctionIdentifier NUMERIC_E = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "e", 0);
    public static final FunctionIdentifier NUMERIC_PI = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "pi", 0);

    public static final FunctionIdentifier NUMERIC_CEILING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ceiling", 1);
    public static final FunctionIdentifier NUMERIC_FLOOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "floor", 1);
    public static final FunctionIdentifier NUMERIC_ROUND =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "round", 1);
    public static final FunctionIdentifier NUMERIC_ROUND_WITH_ROUND_DIGIT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "round", 2);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "round-half-to-even", 1);
    public static final FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN2 =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "round-half-to-even", 2);
    public static final FunctionIdentifier NUMERIC_TRUNC =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "trunc", 2);

    // binary functions
    public static final FunctionIdentifier BINARY_LENGTH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "binary-length", 1);
    public static final FunctionIdentifier PARSE_BINARY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "parse-binary", 2);
    public static final FunctionIdentifier PRINT_BINARY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "print-binary", 2);
    public static final FunctionIdentifier BINARY_CONCAT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "binary-concat", 1);
    public static final FunctionIdentifier SUBBINARY_FROM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sub-binary", 2);
    public static final FunctionIdentifier SUBBINARY_FROM_TO =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sub-binary", 3);
    public static final FunctionIdentifier FIND_BINARY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "find-binary", 2);
    public static final FunctionIdentifier FIND_BINARY_FROM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "find-binary", 3);

    // bitwise functions
    public static final FunctionIdentifier BIT_AND =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitand", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier BIT_OR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier BIT_XOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitxor", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier BIT_NOT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitnot", 1);
    public static final FunctionIdentifier BIT_COUNT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitcount", 1);
    public static final FunctionIdentifier BIT_SET = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitset", 2);
    public static final FunctionIdentifier BIT_CLEAR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitclear", 2);
    public static final FunctionIdentifier BIT_SHIFT_WITHOUT_ROTATE_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitshift", 2);
    public static final FunctionIdentifier BIT_SHIFT_WITH_ROTATE_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bitshift", 3);
    public static final FunctionIdentifier BIT_TEST_WITHOUT_ALL_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bittest", 2);
    public static final FunctionIdentifier BIT_TEST_WITH_ALL_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "bittest", 3);
    public static final FunctionIdentifier IS_BIT_SET_WITHOUT_ALL_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "isbitset", 2);
    public static final FunctionIdentifier IS_BIT_SET_WITH_ALL_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "isbitset", 3);

    // String functions
    public static final FunctionIdentifier STRING_EQUAL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string-equal", 2);
    public static final FunctionIdentifier STRING_MATCHES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "matches", 2);
    public static final FunctionIdentifier STRING_MATCHES_WITH_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "matches", 3);
    public static final FunctionIdentifier STRING_REGEXP_LIKE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "regexp-like", 2);
    public static final FunctionIdentifier STRING_REGEXP_LIKE_WITH_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "regexp-like", 3);
    public static final FunctionIdentifier STRING_REGEXP_POSITION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "regexp-position", 2);
    public static final FunctionIdentifier STRING_REGEXP_POSITION_WITH_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "regexp-position", 3);
    public static final FunctionIdentifier STRING_REGEXP_REPLACE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "regexp-replace", 3);
    public static final FunctionIdentifier STRING_REGEXP_REPLACE_WITH_FLAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "regexp-replace", 4);
    public static final FunctionIdentifier STRING_LOWERCASE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "lowercase", 1);
    public static final FunctionIdentifier STRING_UPPERCASE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "uppercase", 1);
    public static final FunctionIdentifier STRING_INITCAP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "initcap", 1);
    public static final FunctionIdentifier STRING_TRIM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "trim", 1);
    public static final FunctionIdentifier STRING_LTRIM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ltrim", 1);
    public static final FunctionIdentifier STRING_RTRIM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "rtrim", 1);
    public static final FunctionIdentifier STRING_TRIM2 =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "trim", 2);
    public static final FunctionIdentifier STRING_LTRIM2 =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ltrim", 2);
    public static final FunctionIdentifier STRING_RTRIM2 =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "rtrim", 2);
    public static final FunctionIdentifier STRING_POSITION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "position", 2);
    public static final FunctionIdentifier STRING_REPLACE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "replace", 3);
    public static final FunctionIdentifier STRING_REPLACE_WITH_LIMIT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "replace", 4);
    public static final FunctionIdentifier STRING_REVERSE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "reverse", 1);
    public static final FunctionIdentifier STRING_LENGTH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string-length", 1);
    public static final FunctionIdentifier STRING_LIKE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "like", 2);
    public static final FunctionIdentifier STRING_CONTAINS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "contains", 2);
    public static final FunctionIdentifier STRING_STARTS_WITH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "starts-with", 2);
    public static final FunctionIdentifier STRING_ENDS_WITH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ends-with", 2);
    public static final FunctionIdentifier SUBSTRING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "substring", 3);
    public static final FunctionIdentifier SUBSTRING2 =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "substring", 2);
    public static final FunctionIdentifier SUBSTRING_BEFORE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "substring-before", 2);
    public static final FunctionIdentifier SUBSTRING_AFTER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "substring-after", 2);
    public static final FunctionIdentifier STRING_TO_CODEPOINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string-to-codepoint", 1);
    public static final FunctionIdentifier CODEPOINT_TO_STRING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "codepoint-to-string", 1);
    public static final FunctionIdentifier STRING_CONCAT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string-concat", 1);
    public static final FunctionIdentifier STRING_JOIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string-join", 2);
    public static final FunctionIdentifier STRING_REPEAT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "repeat", 2);
    public static final FunctionIdentifier STRING_SPLIT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "split", 2);

    public static final FunctionIdentifier DATASET = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "dataset", 1);
    public static final FunctionIdentifier FEED_COLLECT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "feed-collect", 6);
    public static final FunctionIdentifier FEED_INTERCEPT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "feed-intercept", 1);

    public static final FunctionIdentifier INDEX_SEARCH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "index-search", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier MAKE_FIELD_INDEX_HANDLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "make-field-index-handle", 2);
    public static final FunctionIdentifier MAKE_FIELD_NESTED_HANDLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "make-field-nested-handle", 3);
    public static final FunctionIdentifier MAKE_FIELD_NAME_HANDLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "make-field-name-handle", 1);

    // aggregate functions
    public static final FunctionIdentifier LISTIFY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "listify", 1);
    public static final FunctionIdentifier AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-avg", 1);
    public static final FunctionIdentifier COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count", 1);
    public static final FunctionIdentifier SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sum", 1);
    public static final FunctionIdentifier LOCAL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sum", 1);
    public static final FunctionIdentifier INTERMEDIATE_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-sum", 1);
    public static final FunctionIdentifier GLOBAL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sum", 1);
    public static final FunctionIdentifier MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-max", 1);
    public static final FunctionIdentifier LOCAL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-max", 1);
    public static final FunctionIdentifier INTERMEDIATE_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-max", 1);
    public static final FunctionIdentifier GLOBAL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-max", 1);
    public static final FunctionIdentifier MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-min", 1);
    public static final FunctionIdentifier LOCAL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-min", 1);
    public static final FunctionIdentifier INTERMEDIATE_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-min", 1);
    public static final FunctionIdentifier GLOBAL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-min", 1);
    public static final FunctionIdentifier GLOBAL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-avg", 1);
    public static final FunctionIdentifier INTERMEDIATE_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-avg", 1);
    public static final FunctionIdentifier LOCAL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-avg", 1);
    public static final FunctionIdentifier FIRST_ELEMENT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-first-element", 1);
    public static final FunctionIdentifier LOCAL_FIRST_ELEMENT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-first-element", 1);
    public static final FunctionIdentifier LAST_ELEMENT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-last-element", 1);
    public static final FunctionIdentifier STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-stddev_samp", 1);
    public static final FunctionIdentifier GLOBAL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-stddev_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-stddev_samp", 1);
    public static final FunctionIdentifier LOCAL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-stddev_samp", 1);
    public static final FunctionIdentifier LOCAL_SAMPLING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sampling", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RANGE_MAP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-range-map", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-stddev_pop", 1);
    public static final FunctionIdentifier GLOBAL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-stddev_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-stddev_pop", 1);
    public static final FunctionIdentifier LOCAL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-stddev_pop", 1);
    public static final FunctionIdentifier VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-var_samp", 1);
    public static final FunctionIdentifier GLOBAL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-var_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-var_samp", 1);
    public static final FunctionIdentifier LOCAL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-var_samp", 1);
    public static final FunctionIdentifier VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-var_pop", 1);
    public static final FunctionIdentifier GLOBAL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-var_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-var_pop", 1);
    public static final FunctionIdentifier LOCAL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-var_pop", 1);
    public static final FunctionIdentifier SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-skewness", 1);
    public static final FunctionIdentifier GLOBAL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-skewness", 1);
    public static final FunctionIdentifier INTERMEDIATE_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-skewness", 1);
    public static final FunctionIdentifier LOCAL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-skewness", 1);
    public static final FunctionIdentifier KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-kurtosis", 1);
    public static final FunctionIdentifier GLOBAL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-kurtosis", 1);
    public static final FunctionIdentifier INTERMEDIATE_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-kurtosis", 1);
    public static final FunctionIdentifier LOCAL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-kurtosis", 1);
    public static final FunctionIdentifier NULL_WRITER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-null-writer", 1);

    public static final FunctionIdentifier SCALAR_ARRAYAGG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "arrayagg", 1);
    public static final FunctionIdentifier SCALAR_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "avg", 1);
    public static final FunctionIdentifier SCALAR_COUNT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "count", 1);
    public static final FunctionIdentifier SCALAR_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sum", 1);
    public static final FunctionIdentifier SCALAR_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "max", 1);
    public static final FunctionIdentifier SCALAR_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "min", 1);
    public static final FunctionIdentifier SCALAR_FIRST_ELEMENT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "first-element", 1);
    public static final FunctionIdentifier SCALAR_LOCAL_FIRST_ELEMENT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-first-element", 1);
    public static final FunctionIdentifier SCALAR_LAST_ELEMENT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "last-element", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "stddev_samp", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "stddev_pop", 1);
    public static final FunctionIdentifier SCALAR_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "var_samp", 1);
    public static final FunctionIdentifier SCALAR_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "var_pop", 1);
    public static final FunctionIdentifier SCALAR_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "skewness", 1);
    public static final FunctionIdentifier SCALAR_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "kurtosis", 1);

    // serializable aggregate functions
    public static final FunctionIdentifier SERIAL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "avg-serial", 1);
    public static final FunctionIdentifier SERIAL_COUNT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "count-serial", 1);
    public static final FunctionIdentifier SERIAL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sum-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-stddev_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-kurtosis-serial", 1);

    // distinct aggregate functions
    public static final FunctionIdentifier LISTIFY_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "listify-distinct", 1);
    public static final FunctionIdentifier SCALAR_ARRAYAGG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "arrayagg-distinct", 1);
    public static final FunctionIdentifier COUNT_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count-distinct", 1);
    public static final FunctionIdentifier SCALAR_COUNT_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "count-distinct", 1);
    public static final FunctionIdentifier SUM_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sum-distinct", 1);
    public static final FunctionIdentifier SCALAR_SUM_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sum-distinct", 1);
    public static final FunctionIdentifier AVG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-avg-distinct", 1);
    public static final FunctionIdentifier SCALAR_AVG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "avg-distinct", 1);
    public static final FunctionIdentifier MAX_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-max-distinct", 1);
    public static final FunctionIdentifier SCALAR_MAX_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "max-distinct", 1);
    public static final FunctionIdentifier MIN_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-min-distinct", 1);
    public static final FunctionIdentifier SCALAR_MIN_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "min-distinct", 1);
    public static final FunctionIdentifier STDDEV_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-stddev_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "stddev_samp-distinct", 1);
    public static final FunctionIdentifier STDDEV_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-stddev_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_STDDEV_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "stddev_pop-distinct", 1);
    public static final FunctionIdentifier VAR_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-var_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_VAR_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "var_samp-distinct", 1);
    public static final FunctionIdentifier VAR_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-var_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_VAR_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "var_pop-distinct", 1);
    public static final FunctionIdentifier SKEWNESS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-skewness-distinct", 1);
    public static final FunctionIdentifier SCALAR_SKEWNESS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "skewness-distinct", 1);
    public static final FunctionIdentifier KURTOSIS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-kurtosis-distinct", 1);
    public static final FunctionIdentifier SCALAR_KURTOSIS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "kurtosis-distinct", 1);

    // sql aggregate functions
    public static final FunctionIdentifier SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-avg", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-avg", 1);
    public static final FunctionIdentifier SQL_COUNT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-count", 1);
    public static final FunctionIdentifier SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-sum", 1);
    public static final FunctionIdentifier LOCAL_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-sum", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-sql-sum", 1);
    public static final FunctionIdentifier GLOBAL_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-sum", 1);
    public static final FunctionIdentifier SQL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-max", 1);
    public static final FunctionIdentifier LOCAL_SQL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-max", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-sql-max", 1);
    public static final FunctionIdentifier GLOBAL_SQL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-max", 1);
    public static final FunctionIdentifier SQL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-min", 1);
    public static final FunctionIdentifier LOCAL_SQL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-min", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-intermediate-sql-min", 1);
    public static final FunctionIdentifier GLOBAL_SQL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-min", 1);
    public static final FunctionIdentifier GLOBAL_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-avg", 1);
    public static final FunctionIdentifier LOCAL_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-avg", 1);
    public static final FunctionIdentifier SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-stddev_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-stddev_samp", 1);
    public static final FunctionIdentifier GLOBAL_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-stddev_samp", 1);
    public static final FunctionIdentifier LOCAL_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-stddev_samp", 1);
    public static final FunctionIdentifier SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-stddev_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-stddev_pop", 1);
    public static final FunctionIdentifier GLOBAL_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-stddev_pop", 1);
    public static final FunctionIdentifier LOCAL_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-stddev_pop", 1);
    public static final FunctionIdentifier SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-var_samp", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-var_samp", 1);
    public static final FunctionIdentifier GLOBAL_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-var_samp", 1);
    public static final FunctionIdentifier LOCAL_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-var_samp", 1);
    public static final FunctionIdentifier SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-var_pop", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-var_pop", 1);
    public static final FunctionIdentifier GLOBAL_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-var_pop", 1);
    public static final FunctionIdentifier LOCAL_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-var_pop", 1);
    public static final FunctionIdentifier SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-skewness", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-skewness", 1);
    public static final FunctionIdentifier GLOBAL_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-skewness", 1);
    public static final FunctionIdentifier LOCAL_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-skewness", 1);
    public static final FunctionIdentifier SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-kurtosis", 1);
    public static final FunctionIdentifier INTERMEDIATE_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-agg-sql-kurtosis", 1);
    public static final FunctionIdentifier GLOBAL_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-global-sql-kurtosis", 1);
    public static final FunctionIdentifier LOCAL_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-local-sql-kurtosis", 1);

    public static final FunctionIdentifier SCALAR_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-avg", 1);
    public static final FunctionIdentifier SCALAR_SQL_COUNT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-count", 1);
    public static final FunctionIdentifier SCALAR_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-sum", 1);
    public static final FunctionIdentifier SCALAR_SQL_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-max", 1);
    public static final FunctionIdentifier SCALAR_SQL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-min", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-stddev_samp", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-stddev_pop", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-var_samp", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-var_pop", 1);
    public static final FunctionIdentifier SCALAR_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-skewness", 1);
    public static final FunctionIdentifier SCALAR_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-kurtosis", 1);

    // serializable sql aggregate functions
    public static final FunctionIdentifier SERIAL_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_COUNT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-count-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_SUM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-sum-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_AVG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-avg-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_STDDEV_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-stddev-serial_samp", 1);
    public static final FunctionIdentifier SERIAL_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_STDDEV_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-stddev_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_VAR_SAMP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-var_samp-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_VAR_POP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-var_pop-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_SKEWNESS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-skewness-serial", 1);
    public static final FunctionIdentifier SERIAL_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_GLOBAL_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "global-sql-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_INTERMEDIATE_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "intermediate-sql-kurtosis-serial", 1);
    public static final FunctionIdentifier SERIAL_LOCAL_SQL_KURTOSIS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "local-sql-kurtosis-serial", 1);

    // distinct sql aggregate functions
    public static final FunctionIdentifier SQL_COUNT_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-count-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_COUNT_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-count-distinct", 1);
    public static final FunctionIdentifier SQL_SUM_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-sum-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_SUM_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-sum-distinct", 1);
    public static final FunctionIdentifier SQL_AVG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-avg-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_AVG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-avg-distinct", 1);
    public static final FunctionIdentifier SQL_MAX_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-max-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_MAX_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-max-distinct", 1);
    public static final FunctionIdentifier SQL_MIN_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-min-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_MIN_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-min-distinct", 1);
    public static final FunctionIdentifier SQL_STDDEV_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-stddev_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-stddev_samp-distinct", 1);
    public static final FunctionIdentifier SQL_STDDEV_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-stddev_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_STDDEV_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-stddev_pop-distinct", 1);
    public static final FunctionIdentifier SQL_VAR_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-var_samp-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_SAMP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-var_samp-distinct", 1);
    public static final FunctionIdentifier SQL_VAR_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-var_pop-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_VAR_POP_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-var_pop-distinct", 1);
    public static final FunctionIdentifier SQL_SKEWNESS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-skewness-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_SKEWNESS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-skewness-distinct", 1);
    public static final FunctionIdentifier SQL_KURTOSIS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sql-kurtosis-distinct", 1);
    public static final FunctionIdentifier SCALAR_SQL_KURTOSIS_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sql-kurtosis-distinct", 1);

    // window functions
    public static final FunctionIdentifier CUME_DIST =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cume_dist", 0);
    public static final FunctionIdentifier CUME_DIST_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cume-dist-impl", 0);
    public static final FunctionIdentifier DENSE_RANK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "dense_rank", 0);
    public static final FunctionIdentifier DENSE_RANK_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "dense-rank-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier FIRST_VALUE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "first_value", 1);
    public static final FunctionIdentifier FIRST_VALUE_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "first-value-impl", 2);
    public static final FunctionIdentifier LAG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "lag", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier LAG_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "lag-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier LAST_VALUE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "last_value", 1);
    public static final FunctionIdentifier LAST_VALUE_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "last-value-impl", 2);
    public static final FunctionIdentifier LEAD =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "lead", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier LEAD_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "lead-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier NTH_VALUE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "nth_value", 2);
    public static final FunctionIdentifier NTH_VALUE_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "nth-value-impl", 3);
    public static final FunctionIdentifier NTILE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ntile", 1);
    public static final FunctionIdentifier NTILE_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ntile-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RANK = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "rank", 0);
    public static final FunctionIdentifier RANK_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "rank-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier RATIO_TO_REPORT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ratio_to_report", 1);
    public static final FunctionIdentifier RATIO_TO_REPORT_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ratio-to-report-impl", 2);
    public static final FunctionIdentifier ROW_NUMBER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "row_number", 0);
    public static final FunctionIdentifier ROW_NUMBER_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "row-number-impl", 0);
    public static final FunctionIdentifier PERCENT_RANK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "percent_rank", 0);
    public static final FunctionIdentifier PERCENT_RANK_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "percent-rank-impl", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier WIN_PARTITION_LENGTH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "win_partition_length", 0);
    public static final FunctionIdentifier WIN_PARTITION_LENGTH_IMPL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "win-partition-length-impl", 0);

    // unnesting functions
    public static final FunctionIdentifier SCAN_COLLECTION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "scan-collection", 1);
    public static final FunctionIdentifier SUBSET_COLLECTION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "subset-collection", 3);

    public static final FunctionIdentifier RANGE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "range", 2);

    // fuzzy functions
    public static final FunctionIdentifier FUZZY_EQ =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "fuzzy-eq", 2);

    public static final FunctionIdentifier PREFIX_LEN_JACCARD =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "prefix-len-jaccard", 2);

    public static final FunctionIdentifier SIMILARITY_JACCARD =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity-jaccard", 2);
    public static final FunctionIdentifier SIMILARITY_JACCARD_CHECK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity-jaccard-check", 3);
    public static final FunctionIdentifier SIMILARITY_JACCARD_SORTED =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity-jaccard-sorted", 2);
    public static final FunctionIdentifier SIMILARITY_JACCARD_SORTED_CHECK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity-jaccard-sorted-check", 3);
    public static final FunctionIdentifier SIMILARITY_JACCARD_PREFIX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity-jaccard-prefix", 6);
    public static final FunctionIdentifier SIMILARITY_JACCARD_PREFIX_CHECK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "similarity-jaccard-prefix-check", 6);

    public static final FunctionIdentifier EDIT_DISTANCE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "edit-distance", 2);
    public static final FunctionIdentifier EDIT_DISTANCE_CHECK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "edit-distance-check", 3);
    public static final FunctionIdentifier EDIT_DISTANCE_LIST_IS_FILTERABLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "edit-distance-list-is-filterable", 2);
    public static final FunctionIdentifier EDIT_DISTANCE_STRING_IS_FILTERABLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "edit-distance-string-is-filterable", 4);
    public static final FunctionIdentifier EDIT_DISTANCE_CONTAINS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "edit-distance-contains", 3);

    // full-text
    public static final FunctionIdentifier FULLTEXT_CONTAINS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ftcontains", 3);
    // full-text without any option provided
    public static final FunctionIdentifier FULLTEXT_CONTAINS_WO_OPTION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ftcontains", 2);

    // tokenizers:
    public static final FunctionIdentifier WORD_TOKENS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "word-tokens", 1);
    public static final FunctionIdentifier HASHED_WORD_TOKENS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "hashed-word-tokens", 1);
    public static final FunctionIdentifier COUNTHASHED_WORD_TOKENS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "counthashed-word-tokens", 1);
    public static final FunctionIdentifier GRAM_TOKENS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "gram-tokens", 3);
    public static final FunctionIdentifier HASHED_GRAM_TOKENS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "hashed-gram-tokens", 3);
    public static final FunctionIdentifier COUNTHASHED_GRAM_TOKENS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "counthashed-gram-tokens", 3);

    public static final FunctionIdentifier TID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tid", 0);
    public static final FunctionIdentifier GTID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "gtid", 0);

    // constructors:
    public static final FunctionIdentifier BOOLEAN_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "boolean", 1);
    public static final FunctionIdentifier STRING_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string", 1);
    public static final FunctionIdentifier BINARY_HEX_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "hex", 1);
    public static final FunctionIdentifier BINARY_BASE64_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "base64", 1);
    public static final FunctionIdentifier INT8_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "int8", 1);
    public static final FunctionIdentifier INT16_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "int16", 1);
    public static final FunctionIdentifier INT32_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "int32", 1);
    public static final FunctionIdentifier INT64_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "int64", 1);
    public static final FunctionIdentifier FLOAT_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "float", 1);
    public static final FunctionIdentifier DOUBLE_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "double", 1);
    public static final FunctionIdentifier POINT_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "point", 1);
    public static final FunctionIdentifier POINT3D_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "point3d", 1);
    public static final FunctionIdentifier LINE_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "line", 1);
    public static final FunctionIdentifier CIRCLE_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "circle", 1);
    public static final FunctionIdentifier RECTANGLE_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "rectangle", 1);
    public static final FunctionIdentifier POLYGON_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "polygon", 1);
    public static final FunctionIdentifier TIME_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "time", 1);
    public static final FunctionIdentifier DATE_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "date", 1);
    public static final FunctionIdentifier DATETIME_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "datetime", 1);
    public static final FunctionIdentifier DURATION_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "duration", 1);
    public static final FunctionIdentifier UUID_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "uuid", 1);

    public static final FunctionIdentifier YEAR_MONTH_DURATION_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "year-month-duration", 1);
    public static final FunctionIdentifier DAY_TIME_DURATION_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "day-time-duration", 1);

    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-start-from-date", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-start-from-time", 2);
    public static final FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-start-from-datetime", 2);
    public static final FunctionIdentifier INTERVAL_BEFORE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-before", 2);
    public static final FunctionIdentifier INTERVAL_AFTER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-after", 2);
    public static final FunctionIdentifier INTERVAL_MEETS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-meets", 2);
    public static final FunctionIdentifier INTERVAL_MET_BY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-met-by", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-overlaps", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPPED_BY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-overlapped-by", 2);
    public static final FunctionIdentifier INTERVAL_OVERLAPPING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-overlapping", 2);
    public static final FunctionIdentifier INTERVAL_STARTS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-starts", 2);
    public static final FunctionIdentifier INTERVAL_STARTED_BY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-started-by", 2);
    public static final FunctionIdentifier INTERVAL_COVERS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-covers", 2);
    public static final FunctionIdentifier INTERVAL_COVERED_BY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-covered-by", 2);
    public static final FunctionIdentifier INTERVAL_ENDS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-ends", 2);
    public static final FunctionIdentifier INTERVAL_ENDED_BY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-ended-by", 2);
    public static final FunctionIdentifier CURRENT_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "current-time", 0);
    public static final FunctionIdentifier CURRENT_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "current-date", 0);
    public static final FunctionIdentifier CURRENT_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "current-datetime", 0);
    public static final FunctionIdentifier DURATION_EQUAL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "duration-equal", 2);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_GREATER_THAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "year-month-duration-greater-than", 2);
    public static final FunctionIdentifier YEAR_MONTH_DURATION_LESS_THAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "year-month-duration-less-than", 2);
    public static final FunctionIdentifier DAY_TIME_DURATION_GREATER_THAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "day-time-duration-greater-than", 2);
    public static final FunctionIdentifier DAY_TIME_DURATION_LESS_THAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "day-time-duration-less-than", 2);
    public static final FunctionIdentifier DURATION_FROM_MONTHS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "duration-from-months", 1);
    public static final FunctionIdentifier MONTHS_FROM_YEAR_MONTH_DURATION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "months-from-year-month-duration", 1);
    public static final FunctionIdentifier DURATION_FROM_MILLISECONDS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "duration-from-ms", 1);
    public static final FunctionIdentifier MILLISECONDS_FROM_DAY_TIME_DURATION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "ms-from-day-time-duration", 1);

    public static final FunctionIdentifier GET_YEAR_MONTH_DURATION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-year-month-duration", 1);
    public static final FunctionIdentifier GET_DAY_TIME_DURATION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-day-time-duration", 1);
    public static final FunctionIdentifier DURATION_FROM_INTERVAL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "duration-from-interval", 1);

    // spatial
    public static final FunctionIdentifier CREATE_POINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-point", 2);
    public static final FunctionIdentifier CREATE_LINE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-line", 2);
    public static final FunctionIdentifier CREATE_POLYGON =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-polygon", 1);
    public static final FunctionIdentifier CREATE_CIRCLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-circle", 2);
    public static final FunctionIdentifier CREATE_RECTANGLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-rectangle", 2);
    public static final FunctionIdentifier SPATIAL_INTERSECT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "spatial-intersect", 2);
    public static final FunctionIdentifier SPATIAL_AREA =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "spatial-area", 1);
    public static final FunctionIdentifier SPATIAL_DISTANCE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "spatial-distance", 2);
    public static final FunctionIdentifier CREATE_MBR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-mbr", 3);
    public static final FunctionIdentifier SPATIAL_CELL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "spatial-cell", 4);
    public static final FunctionIdentifier SWITCH_CASE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "switch-case", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier SLEEP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sleep", 2);
    public static final FunctionIdentifier INJECT_FAILURE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "inject-failure", 2);
    public static final FunctionIdentifier FLOW_RECORD =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "flow-object", 1);
    public static final FunctionIdentifier CAST_TYPE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cast", 1);
    public static final FunctionIdentifier CAST_TYPE_LAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "cast-lax", 1);

    public static final FunctionIdentifier CREATE_UUID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-uuid", 0);
    public static final FunctionIdentifier UUID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "uuid", 0);
    public static final FunctionIdentifier CREATE_QUERY_UID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "create-query-uid", 0);
    public static final FunctionIdentifier RANDOM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "random", 0);
    public static final FunctionIdentifier RANDOM_WITH_SEED =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "random", 1);

    //Geo
    public static final FunctionIdentifier ST_AREA = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-area", 1);
    public static final FunctionIdentifier ST_MAKE_POINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-make-point", 2);
    public static final FunctionIdentifier ST_MAKE_POINT3D =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-make-point", 3);
    public static final FunctionIdentifier ST_MAKE_POINT3D_M =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-make-point", 4);
    public static final FunctionIdentifier ST_INTERSECTS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-intersects", 2);
    public static final FunctionIdentifier ST_UNION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-union", 2);
    public static final FunctionIdentifier ST_IS_COLLECTION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-is-collection", 1);
    public static final FunctionIdentifier ST_CONTAINS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-contains", 2);
    public static final FunctionIdentifier ST_CROSSES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-crosses", 2);
    public static final FunctionIdentifier ST_DISJOINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-disjoint", 2);
    public static final FunctionIdentifier ST_EQUALS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-equals", 2);
    public static final FunctionIdentifier ST_OVERLAPS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-overlaps", 2);
    public static final FunctionIdentifier ST_TOUCHES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-touches", 2);
    public static final FunctionIdentifier ST_WITHIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-within", 2);
    public static final FunctionIdentifier ST_IS_EMPTY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-is-empty", 1);
    public static final FunctionIdentifier ST_IS_SIMPLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-is-simple", 1);
    public static final FunctionIdentifier ST_COORD_DIM =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-coord-dim", 1);
    public static final FunctionIdentifier ST_DIMENSION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-dimension", 1);
    public static final FunctionIdentifier GEOMETRY_TYPE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "geometry-type", 1);
    public static final FunctionIdentifier ST_M = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-m", 1);
    public static final FunctionIdentifier ST_N_RINGS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-n-rings", 1);
    public static final FunctionIdentifier ST_N_POINTS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-n-points", 1);
    public static final FunctionIdentifier ST_NUM_GEOMETRIIES =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-num-geometries", 1);
    public static final FunctionIdentifier ST_NUM_INTERIOR_RINGS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-num-interior-rings", 1);
    public static final FunctionIdentifier ST_SRID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-srid", 1);
    public static final FunctionIdentifier ST_X = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-x", 1);
    public static final FunctionIdentifier ST_Y = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-y", 1);
    public static final FunctionIdentifier ST_X_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-x-max", 1);
    public static final FunctionIdentifier ST_X_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-x-min", 1);
    public static final FunctionIdentifier ST_Y_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-y-max", 1);
    public static final FunctionIdentifier ST_Y_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-y-min", 1);
    public static final FunctionIdentifier ST_Z = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-z", 1);
    public static final FunctionIdentifier ST_Z_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-z-min", 1);
    public static final FunctionIdentifier ST_Z_MAX =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-z-max", 1);
    public static final FunctionIdentifier ST_AS_BINARY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-as-binary", 1);
    public static final FunctionIdentifier ST_AS_TEXT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-as-text", 1);
    public static final FunctionIdentifier ST_AS_GEOJSON =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-as-geojson", 1);
    public static final FunctionIdentifier ST_DISTANCE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-distance", 2);
    public static final FunctionIdentifier ST_LENGTH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-length", 1);
    public static final FunctionIdentifier SCALAR_ST_UNION_AGG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-union", 1);
    public static final FunctionIdentifier SCALAR_ST_UNION_AGG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-union-distinct", 1);
    public static final FunctionIdentifier ST_UNION_AGG =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-union-agg", 1);
    public static final FunctionIdentifier ST_UNION_AGG_DISTINCT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-union-agg-distinct", 1);
    public static final FunctionIdentifier ST_GEOM_FROM_TEXT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-geom-from-text", 1);
    public static final FunctionIdentifier ST_GEOM_FROM_TEXT_SRID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-geom-from-text", 2);
    public static final FunctionIdentifier ST_GEOM_FROM_WKB =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-geom-from-wkb", 1);
    public static final FunctionIdentifier ST_GEOM_FROM_WKB_SRID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-geom-from-wkb", 2);
    public static final FunctionIdentifier ST_LINE_FROM_MULTIPOINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-line-from-multipoint", 1);
    public static final FunctionIdentifier ST_MAKE_ENVELOPE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-make-envelope", 5);
    public static final FunctionIdentifier ST_IS_CLOSED =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-is-closed", 1);
    public static final FunctionIdentifier ST_IS_RING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-is-ring", 1);
    public static final FunctionIdentifier ST_RELATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-relate", 3);
    public static final FunctionIdentifier ST_BOUNDARY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-boundary", 1);
    public static final FunctionIdentifier ST_END_POINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-end-point", 1);
    public static final FunctionIdentifier ST_ENVELOPE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-envelope", 1);
    public static final FunctionIdentifier ST_EXTERIOR_RING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-exterior-ring", 1);
    public static final FunctionIdentifier ST_GEOMETRY_N =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-geometry-n", 2);
    public static final FunctionIdentifier ST_INTERIOR_RING_N =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-interior-ring-n", 2);
    public static final FunctionIdentifier ST_POINT_N =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-point-n", 2);
    public static final FunctionIdentifier ST_START_POINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-start-point", 1);
    public static final FunctionIdentifier ST_DIFFERENCE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-difference", 2);
    public static final FunctionIdentifier ST_INTERSECTION =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-intersection", 2);
    public static final FunctionIdentifier ST_SYM_DIFFERENCE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-sym-difference", 2);
    public static final FunctionIdentifier ST_POLYGONIZE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "st-polygonize", 1);

    // Spatial and temporal type accessors
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_YEAR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-year", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MONTH =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-month", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_DAY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-day", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_HOUR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-hour", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-minute", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_SEC =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-second", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MILLISEC =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-millisecond", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-start", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-end", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-start-datetime", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-end-datetime", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-start-date", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-end-date", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-start-time", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-interval-end-time", 1);
    public static final FunctionIdentifier INTERVAL_BIN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "interval-bin", 3);
    public static final FunctionIdentifier OVERLAP_BINS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "overlap-bins", 3);
    public static final FunctionIdentifier GET_OVERLAPPING_INTERVAL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-overlapping-interval", 2);

    // Temporal functions
    public static final FunctionIdentifier UNIX_TIME_FROM_DATE_IN_DAYS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "unix-time-from-date-in-days", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_TIME_IN_MS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "unix-time-from-time-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_MS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "unix-time-from-datetime-in-ms", 1);
    public final static FunctionIdentifier UNIX_TIME_FROM_DATETIME_IN_SECS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "unix-time-from-datetime-in-secs", 1);
    public static final FunctionIdentifier DATE_FROM_UNIX_TIME_IN_DAYS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "date-from-unix-time-in-days", 1);
    public static final FunctionIdentifier DATE_FROM_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-date-from-datetime", 1);
    public static final FunctionIdentifier TIME_FROM_UNIX_TIME_IN_MS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "time-from-unix-time-in-ms", 1);
    public static final FunctionIdentifier TIME_FROM_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-time-from-datetime", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_MS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "datetime-from-unix-time-in-ms", 1);
    public static final FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_SECS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "datetime-from-unix-time-in-secs", 1);
    public static final FunctionIdentifier DATETIME_FROM_DATE_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "datetime-from-date-time", 2);
    public static final FunctionIdentifier CALENDAR_DURATION_FROM_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "calendar-duration-from-datetime", 2);
    public static final FunctionIdentifier CALENDAR_DURATION_FROM_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "calendar-duration-from-date", 2);
    public static final FunctionIdentifier ADJUST_TIME_FOR_TIMEZONE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "adjust-time-for-timezone", 2);
    public static final FunctionIdentifier ADJUST_DATETIME_FOR_TIMEZONE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "adjust-datetime-for-timezone", 2);
    public static final FunctionIdentifier DAY_OF_WEEK =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "day-of-week", 1);
    public static final FunctionIdentifier PARSE_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "parse-date", 2);
    public static final FunctionIdentifier PARSE_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "parse-time", 2);
    public static final FunctionIdentifier PARSE_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "parse-datetime", 2);
    public static final FunctionIdentifier PRINT_DATE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "print-date", 2);
    public static final FunctionIdentifier PRINT_TIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "print-time", 2);
    public static final FunctionIdentifier PRINT_DATETIME =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "print-datetime", 2);

    public static final FunctionIdentifier GET_POINT_X_COORDINATE_ACCESSOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-x", 1);
    public static final FunctionIdentifier GET_POINT_Y_COORDINATE_ACCESSOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-y", 1);
    public static final FunctionIdentifier GET_CIRCLE_RADIUS_ACCESSOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-radius", 1);
    public static final FunctionIdentifier GET_CIRCLE_CENTER_ACCESSOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-center", 1);
    public static final FunctionIdentifier GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-points", 1);

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
    public static final FunctionIdentifier IS_UNKNOWN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-unknown", 1);
    public static final FunctionIdentifier IS_ATOMIC =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-atomic", 1);
    public static final FunctionIdentifier IS_BOOLEAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-boolean", 1);
    public static final FunctionIdentifier IS_NUMBER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-number", 1);
    public static final FunctionIdentifier IS_STRING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-string", 1);
    public static final FunctionIdentifier IS_ARRAY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-array", 1);
    public static final FunctionIdentifier IS_OBJECT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-object", 1);

    public static final FunctionIdentifier IS_SYSTEM_NULL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-system-null", 1);
    public static final FunctionIdentifier CHECK_UNKNOWN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "check-unknown", 1);
    public static final FunctionIdentifier COLLECTION_TO_SEQUENCE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "collection-to-sequence", 1);

    public static final FunctionIdentifier IF_MISSING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-missing", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_NULL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_MISSING_OR_NULL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-missing-or-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_SYSTEM_NULL =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-system-null", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_INF =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-inf", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_NAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-nan", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier IF_NAN_OR_INF =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "if-nan-or-inf", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier MISSING_IF =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "missing-if", 2);
    public static final FunctionIdentifier NULL_IF = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "null-if", 2);
    public static final FunctionIdentifier NAN_IF = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "nan-if", 2);
    public static final FunctionIdentifier POSINF_IF =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "posinf-if", 2);
    public static final FunctionIdentifier NEGINF_IF =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "neginf-if", 2);

    public static final FunctionIdentifier TO_ATOMIC =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-atomic", 1);
    public static final FunctionIdentifier TO_ARRAY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-array", 1);
    public static final FunctionIdentifier TO_BIGINT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-bigint", 1);
    public static final FunctionIdentifier TO_BOOLEAN =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-boolean", 1);
    public static final FunctionIdentifier TO_DOUBLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-double", 1);
    public static final FunctionIdentifier TO_NUMBER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-number", 1);
    public static final FunctionIdentifier TO_OBJECT =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-object", 1);
    public static final FunctionIdentifier TO_STRING =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "to-string", 1);

    public static final FunctionIdentifier TREAT_AS_INTEGER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "treat-as-integer", 1);
    public static final FunctionIdentifier IS_NUMERIC_ADD_COMPATIBLE =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "is-numeric-add-compatibe", 1);

    public static final FunctionIdentifier EXTERNAL_LOOKUP =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "external-lookup", FunctionIdentifier.VARARGS);

    public static final FunctionIdentifier GET_JOB_PARAMETER =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-job-param", 1);

    public static final FunctionIdentifier META =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "meta", FunctionIdentifier.VARARGS);
    public static final FunctionIdentifier META_KEY =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "meta-key", FunctionIdentifier.VARARGS);

    public static IFunctionInfo getAsterixFunctionInfo(FunctionIdentifier fid) {
        return registeredFunctions.get(fid);
    }

    public static FunctionInfo lookupFunction(FunctionIdentifier fid) {
        return (FunctionInfo) registeredFunctions.get(fid);
    }

    static {

        // first, take care of Algebricks builtin functions
        addFunction(IS_MISSING, BooleanOnlyTypeComputer.INSTANCE, true);
        addFunction(IS_UNKNOWN, BooleanOnlyTypeComputer.INSTANCE, true);
        addFunction(IS_NULL, BooleanOrMissingTypeComputer.INSTANCE, true);
        addFunction(IS_SYSTEM_NULL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(IS_ATOMIC, ABooleanTypeComputer.INSTANCE, true);
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

        // deep equality
        addFunction(DEEP_EQUAL, BooleanFunctionTypeComputer.INSTANCE, true);

        // and then, Asterix builtin functions
        addPrivateFunction(CHECK_UNKNOWN, NotUnknownTypeComputer.INSTANCE, true);
        addPrivateFunction(ANY_COLLECTION_MEMBER, CollectionMemberResultType.INSTANCE_MISSABLE, true);
        addFunction(BOOLEAN_CONSTRUCTOR, ABooleanTypeComputer.INSTANCE, true);
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
        addFunction(RANDOM, ADoubleTypeComputer.INSTANCE, false);
        addFunction(RANDOM_WITH_SEED, ADoubleTypeComputer.INSTANCE_NULLABLE, false);

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
        addPrivateFunction(MAKE_FIELD_INDEX_HANDLE, null, true);
        addPrivateFunction(MAKE_FIELD_NAME_HANDLE, null, true);

        addPrivateFunction(NUMERIC_UNARY_MINUS, UnaryMinusTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_SUBTRACT, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_MULTIPLY, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_DIVIDE, NumericDivideTypeComputer.INSTANCE, true);
        addPrivateFunction(NUMERIC_MOD, NumericAddSubMulDivTypeComputer.INSTANCE_NULLABLE, true);
        addPrivateFunction(NUMERIC_DIV, NumericAddSubMulDivTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(NUMERIC_ABS, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ACOS, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ASIN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ATAN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ATAN2, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_DEGREES, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_RADIANS, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_COS, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_COSH, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SIN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SINH, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_TAN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_TANH, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_E, ADoubleTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_EXP, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_LN, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_LOG, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_PI, ADoubleTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_POWER, NumericAddSubMulDivTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SQRT, NumericDoubleOutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_SIGN, NumericInt8OutputFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_CEILING, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_FLOOR, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND, NumericRoundFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_WITH_ROUND_DIGIT, NumericRoundFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN, NumericUnaryFunctionTypeComputer.INSTANCE, true);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN2, NumericRound2TypeComputer.INSTANCE, true);
        addFunction(NUMERIC_TRUNC, NumericRound2TypeComputer.INSTANCE, true);

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
        addFunction(STRING_CONSTRUCTOR, AStringTypeComputer.INSTANCE, true); // TODO
        addFunction(STRING_LIKE, BooleanFunctionTypeComputer.INSTANCE, true);
        addFunction(STRING_CONTAINS, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_TO_CODEPOINT, StringToInt64ListTypeComputer.INSTANCE, true);
        addFunction(CODEPOINT_TO_STRING, AStringTypeComputer.INSTANCE, true); // TODO
        addFunction(STRING_CONCAT, ConcatTypeComputer.INSTANCE_STRING, true); // TODO
        addFunction(SUBSTRING, SubstringTypeComputer.INSTANCE, true); // TODO
        addFunction(SUBSTRING2, AStringTypeComputer.INSTANCE_NULLABLE, true);
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
        addFunction(STRING_REGEXP_REPLACE, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_REGEXP_REPLACE_WITH_FLAG, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_REPLACE, StringStringTypeComputer.INSTANCE, true);
        addFunction(STRING_REPLACE_WITH_LIMIT, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_REVERSE, StringStringTypeComputer.INSTANCE, true);
        addFunction(SUBSTRING_BEFORE, StringStringTypeComputer.INSTANCE, true);
        addFunction(SUBSTRING_AFTER, StringStringTypeComputer.INSTANCE, true);
        addPrivateFunction(STRING_EQUAL, StringBooleanTypeComputer.INSTANCE, true);
        addFunction(STRING_JOIN, StringJoinTypeComputer.INSTANCE, true);
        addFunction(STRING_REPEAT, AStringTypeComputer.INSTANCE_NULLABLE, true);
        addFunction(STRING_SPLIT, StringToStringListTypeComputer.INSTANCE, true);

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
        addFunction(TO_STRING, AStringTypeComputer.INSTANCE, true);

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

        // SUM
        addFunction(SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SUM, ScalarVersionOfAggregateResultType.INSTANCE, true);
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
        addFunction(SCALAR_MAX, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SCALAR_MIN, ScalarVersionOfAggregateResultType.INSTANCE, true);
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

        // SQL SUM
        addFunction(SQL_SUM, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_SUM, ScalarVersionOfAggregateResultType.INSTANCE, true);
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
        addFunction(SCALAR_SQL_MAX, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SCALAR_SQL_MIN, ScalarVersionOfAggregateResultType.INSTANCE, true);
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
        addFunction(SCALAR_SUM_DISTINCT, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SQL_SUM_DISTINCT, NumericSumAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_SUM_DISTINCT, ScalarVersionOfAggregateResultType.INSTANCE, true);

        addFunction(AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SQL_AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_AVG_DISTINCT, NullableDoubleTypeComputer.INSTANCE, true);

        addFunction(MAX_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_MAX_DISTINCT, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SQL_MAX_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_MAX_DISTINCT, ScalarVersionOfAggregateResultType.INSTANCE, true);

        addFunction(MIN_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_MIN_DISTINCT, ScalarVersionOfAggregateResultType.INSTANCE, true);
        addFunction(SQL_MIN_DISTINCT, MinMaxAggTypeComputer.INSTANCE, true);
        addFunction(SCALAR_SQL_MIN_DISTINCT, ScalarVersionOfAggregateResultType.INSTANCE, true);

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
        addPrivateFunction(WIN_PARTITION_LENGTH, AInt64TypeComputer.INSTANCE, false);
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
        addFunctionWithDomain(SPATIAL_INTERSECT, ATypeHierarchy.Domain.SPATIAL, ABooleanTypeComputer.INSTANCE, true);
        addFunction(GET_POINT_X_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_POINT_Y_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_CIRCLE_RADIUS_ACCESSOR, ADoubleTypeComputer.INSTANCE, true);
        addFunction(GET_CIRCLE_CENTER_ACCESSOR, APointTypeComputer.INSTANCE, true);
        addFunction(GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR, OrderedListOfAPointTypeComputer.INSTANCE, true);

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
        addPrivateFunction(ST_UNION_AGG, AGeometryTypeComputer.INSTANCE, true);
        addFunction(ST_POLYGONIZE, AGeometryTypeComputer.INSTANCE, true);

        // Binary functions
        addFunction(BINARY_HEX_CONSTRUCTOR, ABinaryTypeComputer.INSTANCE, true);
        addFunction(BINARY_BASE64_CONSTRUCTOR, ABinaryTypeComputer.INSTANCE, true);

        addPrivateFunction(SUBSET_COLLECTION, SubsetCollectionTypeComputer.INSTANCE, true);
        addFunction(SWITCH_CASE, SwitchCaseComputer.INSTANCE, true);
        addFunction(SLEEP, SleepTypeComputer.INSTANCE, false);
        addPrivateFunction(INJECT_FAILURE, InjectFailureTypeComputer.INSTANCE, true);
        addPrivateFunction(CAST_TYPE, CastTypeComputer.INSTANCE, true);
        addPrivateFunction(CAST_TYPE_LAX, CastTypeLaxComputer.INSTANCE, true);

        addFunction(TID, AInt64TypeComputer.INSTANCE, true);
        addFunction(TIME_CONSTRUCTOR, ATimeTypeComputer.INSTANCE, true);
        addPrivateFunction(TYPE_OF, null, true);
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
        addFunction(RECORD_REMOVE, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(RECORD_RENAME, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(RECORD_UNWRAP, AnyTypeComputer.INSTANCE, true);
        addFunction(RECORD_REPLACE, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(RECORD_ADD, OpenARecordTypeComputer.INSTANCE, true);
        addFunction(RECORD_PUT, OpenARecordTypeComputer.INSTANCE, true);
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

        addPrivateFunction(COLLECTION_TO_SEQUENCE, CollectionToSequenceTypeComputer.INSTANCE, true);

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
    }

    interface BuiltinFunctionProperty {
    }

    public enum WindowFunctionProperty implements BuiltinFunctionProperty {
        /** Whether the order clause is prohibited */
        NO_ORDER_CLAUSE,
        /** Whether the frame clause is prohibited */
        NO_FRAME_CLAUSE,
        /** Whether the first argument is a list */
        HAS_LIST_ARG,
        /** Whether order by expressions must be injected as arguments */
        INJECT_ORDER_ARGS,
        /** Whether a running aggregate requires partition materialization runtime */
        MATERIALIZE_PARTITION,
        /** Whether FROM (FIRST | LAST) modifier is allowed */
        ALLOW_FROM_FIRST_LAST,
        /** Whether (RESPECT | IGNORE) NULLS modifier is allowed */
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
        addWindowFunction(WIN_PARTITION_LENGTH, WIN_PARTITION_LENGTH_IMPL, NO_FRAME_CLAUSE, MATERIALIZE_PARTITION);
    }

    static {
        addUnnestFun(RANGE, true);
        addUnnestFun(SCAN_COLLECTION, false);
        addUnnestFun(SUBSET_COLLECTION, false);
    }

    public enum DataSourceFunctionProperty implements BuiltinFunctionProperty {
        /** Force minimum memory budget if a query only uses this function */
        MIN_MEMORY_BUDGET
    }

    public static void addDatasourceFunction(FunctionIdentifier fi, IFunctionToDataSourceRewriter transformer,
            DataSourceFunctionProperty... properties) {
        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        datasourceFunctions.put(finfo, transformer);
        registerFunctionProperties(finfo, DataSourceFunctionProperty.class, properties);
    }

    public static IFunctionToDataSourceRewriter getDatasourceTransformer(FunctionIdentifier fi) {
        return datasourceFunctions.getOrDefault(getAsterixFunctionInfo(fi), IFunctionToDataSourceRewriter.NOOP);
    }

    public static boolean isBuiltinCompilerFunction(FunctionSignature signature, boolean includePrivateFunctions) {
        FunctionIdentifier fi =
                new FunctionIdentifier(FunctionConstants.ASTERIX_NS, signature.getName(), signature.getArity());
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

    public static FunctionIdentifier getAggregateFunctionForDistinct(FunctionIdentifier distinctVersionOfAggregate) {
        IFunctionInfo finfo =
                distinctToRegularAggregateFunctionMap.get(getAsterixFunctionInfo(distinctVersionOfAggregate));
        return finfo == null ? null : finfo.getFunctionIdentifier();
    }

    public static void addFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer, boolean isFunctional) {
        addFunctionWithDomain(fi, ATypeHierarchy.Domain.ANY, typeComputer, isFunctional);
    }

    public static void addFunctionWithDomain(FunctionIdentifier fi, ATypeHierarchy.Domain funcDomain,
            IResultTypeComputer typeComputer, boolean isFunctional) {
        IFunctionInfo functionInfo = new FunctionInfo(fi, isFunctional);
        builtinPublicFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        registeredFunctions.put(fi, functionInfo);
        registeredFunctionsDomain.put(functionInfo, funcDomain);
    }

    public static void addPrivateFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer,
            boolean isFunctional) {
        IFunctionInfo functionInfo = new FunctionInfo(fi, isFunctional);
        builtinPrivateFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        registeredFunctions.put(fi, functionInfo);
    }

    private static <T extends Enum<T> & BuiltinFunctionProperty> void registerFunctionProperties(IFunctionInfo finfo,
            Class<T> propertyClass, T[] properties) {
        if (properties == null) {
            return;
        }
        Set<T> propertySet = EnumSet.noneOf(propertyClass);
        Collections.addAll(propertySet, properties);
        builtinFunctionProperties.put(finfo, propertySet);
    }

    public static boolean builtinFunctionHasProperty(FunctionIdentifier fi, BuiltinFunctionProperty property) {
        Set<? extends BuiltinFunctionProperty> propertySet = builtinFunctionProperties.get(getAsterixFunctionInfo(fi));
        return propertySet != null && propertySet.contains(property);
    }

    public static void addAgg(FunctionIdentifier fi) {
        builtinAggregateFunctions.add(getAsterixFunctionInfo(fi));
    }

    public static void addLocalAgg(FunctionIdentifier fi, FunctionIdentifier localfi) {
        aggregateToLocalAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(localfi));
    }

    public static void addIntermediateAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToIntermediateAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(globalfi));
    }

    public static void addGlobalAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToGlobalAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(globalfi));
        globalAggregateFunctions.add(getAsterixFunctionInfo(globalfi));
    }

    public static void addUnnestFun(FunctionIdentifier fi, boolean returnsUniqueValues) {
        builtinUnnestingFunctions.put(getAsterixFunctionInfo(fi), returnsUniqueValues);
    }

    public static void addSerialAgg(FunctionIdentifier fi, FunctionIdentifier serialfi) {
        aggregateToSerializableAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(serialfi));
    }

    public static void addScalarAgg(FunctionIdentifier fi, FunctionIdentifier scalarfi) {
        scalarToAggregateFunctionMap.put(getAsterixFunctionInfo(scalarfi), getAsterixFunctionInfo(fi));
    }

    public static void addDistinctAgg(FunctionIdentifier distinctfi, FunctionIdentifier fi) {
        distinctToRegularAggregateFunctionMap.put(getAsterixFunctionInfo(distinctfi), getAsterixFunctionInfo(fi));
    }

    public static void addWindowFunction(FunctionIdentifier sqlfi, FunctionIdentifier winfi,
            WindowFunctionProperty... properties) {
        IFunctionInfo sqlinfo = getAsterixFunctionInfo(sqlfi);
        IFunctionInfo wininfo = getAsterixFunctionInfo(winfi);
        sqlToWindowFunctions.put(sqlinfo, wininfo);
        windowFunctions.add(wininfo);
        registerFunctionProperties(wininfo, WindowFunctionProperty.class, properties);
    }

    public static FunctionIdentifier getWindowFunction(FunctionIdentifier sqlfi) {
        IFunctionInfo finfo = sqlToWindowFunctions.get(getAsterixFunctionInfo(sqlfi));
        return finfo == null ? null : finfo.getFunctionIdentifier();
    }

    public static boolean isWindowFunction(FunctionIdentifier winfi) {
        return windowFunctions.contains(getAsterixFunctionInfo(winfi));
    }

    public static AbstractFunctionCallExpression makeWindowFunctionExpression(FunctionIdentifier winfi,
            List<Mutable<ILogicalExpression>> args) {
        IFunctionInfo finfo = getAsterixFunctionInfo(winfi);
        if (finfo == null) {
            throw new IllegalStateException("no implementation for window function " + finfo);
        }
        return new StatefulFunctionCallExpression(finfo, UnpartitionedPropertyComputer.INSTANCE, args);
    }

    static {
        spatialFilterFunctions.put(getAsterixFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT), SpatialFilterKind.SI);
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
