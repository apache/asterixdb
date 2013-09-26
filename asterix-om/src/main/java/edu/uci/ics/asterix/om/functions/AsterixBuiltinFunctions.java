/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.functions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ACircleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADateTimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADateTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADoubleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AFloatTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AInt32TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AInt64TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ALineTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ANullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.APointTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.APolygonTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ARectangleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AStringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ATimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.BinaryBooleanOrNullFunctionTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.BinaryStringBoolOrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.BinaryStringStringOrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.CastListResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.CastRecordResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ClosedRecordConstructorResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.CollectionToSequenceTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ConcatNonNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.FieldAccessByIndexResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.InjectFailureTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedCollectionMemberResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedFieldAccessByNameResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedGetItemResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedLocalAvgTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedMinMaxAggTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericAddSubMulDivTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericRoundHalfToEven2TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericUnaryFunctionTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericAggTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedSwitchCaseComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedUnaryMinusTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NotNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OpenRecordConstructorResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalABooleanTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalACircleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADateTimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADateTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADayTimeDurationTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADoubleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADurationTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAFloatTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAInt16TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAInt32TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAInt64TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAInt8TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAIntervalTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalALineTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAPoint3DTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAPointTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAPolygonTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalARectangleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAStringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalATemporalInstanceTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalATimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAYearMonthDurationTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListConstructorResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAInt32TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAPointTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAStringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAnyTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.QuadStringStringOrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ScalarVersionOfAggregateResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.Substring2TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.SubstringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.TripleStringBoolOrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.TripleStringStringOrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.UnaryBooleanOrNullFunctionTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.UnaryStringInt32OrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.UnaryStringOrNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.UnorderedListConstructorResultType;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class AsterixBuiltinFunctions {

    public enum SpatialFilterKind {
        SI
    }

    private static final FunctionInfoRepository registeredFunctions = new FunctionInfoRepository();

    // it is supposed to be an identity mapping
    private final static Map<IFunctionInfo, IFunctionInfo> builtinPublicFunctionsSet = new HashMap<IFunctionInfo, IFunctionInfo>();
    private final static Map<IFunctionInfo, IFunctionInfo> builtinPrivateFunctionsSet = new HashMap<IFunctionInfo, IFunctionInfo>();

    private final static Map<IFunctionInfo, IResultTypeComputer> funTypeComputer = new HashMap<IFunctionInfo, IResultTypeComputer>();

    private final static Set<IFunctionInfo> builtinAggregateFunctions = new HashSet<IFunctionInfo>();
    private static final Set<IFunctionInfo> datasetFunctions = new HashSet<IFunctionInfo>();
    private static final Set<IFunctionInfo> similarityFunctions = new HashSet<IFunctionInfo>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToLocalAggregate = new HashMap<IFunctionInfo, IFunctionInfo>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToGlobalAggregate = new HashMap<IFunctionInfo, IFunctionInfo>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToSerializableAggregate = new HashMap<IFunctionInfo, IFunctionInfo>();
    private final static Map<IFunctionInfo, Boolean> builtinUnnestingFunctions = new HashMap<IFunctionInfo, Boolean>();
    private final static Map<IFunctionInfo, IFunctionInfo> scalarToAggregateFunctionMap = new HashMap<IFunctionInfo, IFunctionInfo>();
    private static final Map<IFunctionInfo, SpatialFilterKind> spatialFilterFunctions = new HashMap<IFunctionInfo, SpatialFilterKind>();

    public final static FunctionIdentifier TYPE_OF = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "type-of", 1);
    public final static FunctionIdentifier GET_HANDLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-handle", 2);
    public final static FunctionIdentifier GET_DATA = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-data",
            2);
    public final static FunctionIdentifier EMBED_TYPE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "embed-type", 1);

    public final static FunctionIdentifier GET_ITEM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "get-item",
            2);
    public final static FunctionIdentifier ANY_COLLECTION_MEMBER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "any-collection-member", 1);
    public final static FunctionIdentifier LISTIFY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "listify", 1);
    // public final static FunctionIdentifier BAGIFY = new
    // FunctionIdentifier(ASTERIX_NS, "bagify", 1, true);
    public final static FunctionIdentifier LEN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "len", 1);

    public final static FunctionIdentifier CONCAT_NON_NULL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "concat-non-null", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier EMPTY_STREAM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "empty-stream", 0);
    public final static FunctionIdentifier NON_EMPTY_STREAM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "non-empty-stream", 0);
    public final static FunctionIdentifier ORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "ordered-list-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier UNORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "unordered-list-constructor", FunctionIdentifier.VARARGS);

    // records
    public final static FunctionIdentifier CLOSED_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "closed-record-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier OPEN_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "open-record-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier FIELD_ACCESS_BY_INDEX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "field-access-by-index", 2);
    public final static FunctionIdentifier FIELD_ACCESS_BY_NAME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "field-access-by-name", 2);

    public final static FunctionIdentifier NUMERIC_UNARY_MINUS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-unary-minus", 1);

    public final static FunctionIdentifier NUMERIC_SUBTRACT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-subtract", 2);
    public final static FunctionIdentifier NUMERIC_MULTIPLY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-multiply", 2);
    public final static FunctionIdentifier NUMERIC_DIVIDE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-divide", 2);
    public final static FunctionIdentifier NUMERIC_MOD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-mod", 2);
    public final static FunctionIdentifier NUMERIC_IDIV = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-idiv", 2);
    public final static FunctionIdentifier CARET = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "caret", 2);

    public final static FunctionIdentifier NUMERIC_ABS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "abs", 1);
    public final static FunctionIdentifier NUMERIC_CEILING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ceiling", 1);
    public final static FunctionIdentifier NUMERIC_FLOOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "floor", 1);
    public final static FunctionIdentifier NUMERIC_ROUND = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "round", 1);
    public final static FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "round-half-to-even", 1);
    public final static FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN2 = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "round-half-to-even", 2);
    // String funcitons
    public final static FunctionIdentifier STRING_EQUAL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-equal", 2);
    public final static FunctionIdentifier STRING_START_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "start-with", 2);
    public final static FunctionIdentifier STRING_END_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "end-with", 2);
    public final static FunctionIdentifier STRING_MATCHES = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "matches", 2);
    public final static FunctionIdentifier STRING_MATCHES_WITH_FLAG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "matches", 3);
    public final static FunctionIdentifier STRING_LOWERCASE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "lowercase", 1);
    public final static FunctionIdentifier STRING_REPLACE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "replace", 3);
    public final static FunctionIdentifier STRING_REPLACE_WITH_FLAG = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "replace", 4);
    public final static FunctionIdentifier STRING_LENGTH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-length", 1);
    public final static FunctionIdentifier SUBSTRING2 = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring", 2);
    public final static FunctionIdentifier SUBSTRING_BEFORE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring-before", 2);
    public final static FunctionIdentifier SUBSTRING_AFTER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring-after", 2);
    public final static FunctionIdentifier STRING_TO_CODEPOINT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-to-codepoint", 1);
    public final static FunctionIdentifier CODEPOINT_TO_STRING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "codepoint-to-string", 1);
    public final static FunctionIdentifier STRING_CONCAT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-concat", 1);
    public final static FunctionIdentifier STRING_JOIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string-join", 2);

    public final static FunctionIdentifier DATASET = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "dataset", 1);
    public final static FunctionIdentifier FEED_INGEST = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "feed-ingest", 1);

    public final static FunctionIdentifier INDEX_SEARCH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "index-search", FunctionIdentifier.VARARGS);

    public final static FunctionIdentifier MAKE_FIELD_INDEX_HANDLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "make-field-index-handle", 2);
    public final static FunctionIdentifier MAKE_FIELD_NAME_HANDLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "make-field-name-handle", 1);

    public final static FunctionIdentifier SUBSTRING = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "substring", 3);
    public final static FunctionIdentifier LIKE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "like", 2);
    public final static FunctionIdentifier CONTAINS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "contains",
            2);
    public final static FunctionIdentifier STARTS_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "starts-with", 2);
    public final static FunctionIdentifier ENDS_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ends-with", 2);

    public final static FunctionIdentifier AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-avg", 1);
    public final static FunctionIdentifier COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count", 1);
    public final static FunctionIdentifier SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sum", 1);
    public final static FunctionIdentifier LOCAL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-sum", 1);
    public final static FunctionIdentifier MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-max", 1);
    public final static FunctionIdentifier LOCAL_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-max", 1);
    public final static FunctionIdentifier MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-min", 1);
    public final static FunctionIdentifier LOCAL_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-min", 1);
    public final static FunctionIdentifier GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-global-avg", 1);
    public final static FunctionIdentifier LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "agg-local-avg", 1);

    public final static FunctionIdentifier SCALAR_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "avg", 1);
    public final static FunctionIdentifier SCALAR_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "count",
            1);
    public final static FunctionIdentifier SCALAR_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "sum", 1);
    public final static FunctionIdentifier SCALAR_MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "max", 1);
    public final static FunctionIdentifier SCALAR_MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "min", 1);
    public final static FunctionIdentifier SCALAR_GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-avg", 1);
    public final static FunctionIdentifier SCALAR_LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-avg", 1);

    // serializable aggregate functions
    public final static FunctionIdentifier SERIAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "avg-serial", 1);
    public final static FunctionIdentifier SERIAL_COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "count-serial", 1);
    public final static FunctionIdentifier SERIAL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "sum-serial", 1);
    public final static FunctionIdentifier SERIAL_LOCAL_SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-sum-serial", 1);
    public final static FunctionIdentifier SERIAL_GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-avg-serial", 1);
    public final static FunctionIdentifier SERIAL_LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-avg-serial", 1);

    public final static FunctionIdentifier SCAN_COLLECTION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "scan-collection", 1);
    public final static FunctionIdentifier SUBSET_COLLECTION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "subset-collection", 3);

    public final static FunctionIdentifier RANGE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "range", 2);

    // fuzzy functions:
    public final static FunctionIdentifier FUZZY_EQ = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "fuzzy-eq",
            2);

    public final static FunctionIdentifier PREFIX_LEN_JACCARD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "prefix-len-jaccard", 2);

    public final static FunctionIdentifier SIMILARITY_JACCARD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "similarity-jaccard", 2);
    public final static FunctionIdentifier SIMILARITY_JACCARD_CHECK = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-check", 3);
    public final static FunctionIdentifier SIMILARITY_JACCARD_SORTED = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-sorted", 2);
    public final static FunctionIdentifier SIMILARITY_JACCARD_SORTED_CHECK = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-sorted-check", 3);
    public final static FunctionIdentifier SIMILARITY_JACCARD_PREFIX = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-prefix", 6);
    public final static FunctionIdentifier SIMILARITY_JACCARD_PREFIX_CHECK = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "similarity-jaccard-prefix-check", 6);

    public final static FunctionIdentifier EDIT_DISTANCE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance", 2);
    public final static FunctionIdentifier EDIT_DISTANCE_CHECK = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance-check", 3);
    public final static FunctionIdentifier EDIT_DISTANCE_LIST_IS_FILTERABLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "edit-distance-list-is-filterable", 2);
    public final static FunctionIdentifier EDIT_DISTANCE_STRING_IS_FILTERABLE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "edit-distance-string-is-filterable", 4);

    // tokenizers:
    public final static FunctionIdentifier WORD_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "word-tokens", 1);
    public final static FunctionIdentifier HASHED_WORD_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "hashed-word-tokens", 1);
    public final static FunctionIdentifier COUNTHASHED_WORD_TOKENS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "counthashed-word-tokens", 1);
    public final static FunctionIdentifier GRAM_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "gram-tokens", 3);
    public final static FunctionIdentifier HASHED_GRAM_TOKENS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "hashed-gram-tokens", 3);
    public final static FunctionIdentifier COUNTHASHED_GRAM_TOKENS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "counthashed-gram-tokens", 3);

    public final static FunctionIdentifier TID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tid", 0);
    public final static FunctionIdentifier GTID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "gtid", 0);

    // constructors:
    public final static FunctionIdentifier BOOLEAN_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "boolean", 1);
    public final static FunctionIdentifier NULL_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "null", 1);
    public final static FunctionIdentifier STRING_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "string", 1);
    public final static FunctionIdentifier INT8_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int8", 1);
    public final static FunctionIdentifier INT16_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int16", 1);
    public final static FunctionIdentifier INT32_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int32", 1);
    public final static FunctionIdentifier INT64_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "int64", 1);
    public final static FunctionIdentifier FLOAT_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "float", 1);
    public final static FunctionIdentifier DOUBLE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "double", 1);
    public final static FunctionIdentifier POINT_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "point", 1);
    public final static FunctionIdentifier POINT3D_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "point3d", 1);
    public final static FunctionIdentifier LINE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "line", 1);
    public final static FunctionIdentifier CIRCLE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "circle", 1);
    public final static FunctionIdentifier RECTANGLE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "rectangle", 1);
    public final static FunctionIdentifier POLYGON_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "polygon", 1);
    public final static FunctionIdentifier TIME_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "time", 1);
    public final static FunctionIdentifier DATE_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "date", 1);
    public final static FunctionIdentifier DATETIME_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "datetime", 1);
    public final static FunctionIdentifier DURATION_CONSTRUCTOR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration", 1);

    public final static FunctionIdentifier YEAR_MONTH_DURATION_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "year-month-duration", 1);
    public final static FunctionIdentifier DAY_TIME_DURATION_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "day-time-duration", 1);

    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-from-date", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-from-time", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-from-datetime", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-start-from-date", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-start-from-time", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-start-from-datetime", 2);
    public final static FunctionIdentifier INTERVAL_BEFORE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-before", 2);
    public final static FunctionIdentifier INTERVAL_AFTER = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-after", 2);
    public final static FunctionIdentifier INTERVAL_MEETS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-meets", 2);
    public final static FunctionIdentifier INTERVAL_MET_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-met-by", 2);
    public final static FunctionIdentifier INTERVAL_OVERLAPS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-overlaps", 2);
    public final static FunctionIdentifier INTERVAL_OVERLAPPED_BY = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "interval-overlapped-by", 2);
    public final static FunctionIdentifier OVERLAP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-overlapping", 2);
    public final static FunctionIdentifier INTERVAL_STARTS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-starts", 2);
    public final static FunctionIdentifier INTERVAL_STARTED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-started-by", 2);
    public final static FunctionIdentifier INTERVAL_COVERS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-covers", 2);
    public final static FunctionIdentifier INTERVAL_COVERED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-covered-by", 2);
    public final static FunctionIdentifier INTERVAL_ENDS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-ends", 2);
    public final static FunctionIdentifier INTERVAL_ENDED_BY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-ended-by", 2);
    public final static FunctionIdentifier CURRENT_TIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "current-time", 0);
    public final static FunctionIdentifier CURRENT_DATE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "current-date", 0);
    public final static FunctionIdentifier CURRENT_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "current-datetime", 0);
    public final static FunctionIdentifier DURATION_EQUAL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration-equal", 2);
    public final static FunctionIdentifier YEAR_MONTH_DURATION_GREATER_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "year-month-duration-greater-than", 2);
    public final static FunctionIdentifier YEAR_MONTH_DURATION_LESS_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "year-month-duration-less-than", 2);
    public final static FunctionIdentifier DAY_TIME_DURATION_GREATER_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "day-time-duration-greater-than", 2);
    public final static FunctionIdentifier DAY_TIME_DURATION_LESS_THAN = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "day-time-duration-less-than", 2);
    public final static FunctionIdentifier DURATION_FROM_MONTHS = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "duration-from-months", 1);
    public final static FunctionIdentifier MONTHS_FROM_YEAR_MONTH_DURATION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "months-from-year-month-duration", 1);
    public final static FunctionIdentifier DURATION_FROM_MILLISECONDS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "duration-from-ms", 1);
    public final static FunctionIdentifier MILLISECONDS_FROM_DAY_TIME_DURATION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "ms-from-day-time-duration", 1);

    public final static FunctionIdentifier GET_YEAR_MONTH_DURATION = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-year-month-duration", 1);
    public final static FunctionIdentifier GET_DAY_TIME_DURATION = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-day-time-duration", 1);

    // spatial
    public final static FunctionIdentifier CREATE_POINT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-point", 2);
    public final static FunctionIdentifier CREATE_LINE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-line", 2);
    public final static FunctionIdentifier CREATE_POLYGON = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-polygon", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier CREATE_CIRCLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-circle", 2);
    public final static FunctionIdentifier CREATE_RECTANGLE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-rectangle", 2);
    public final static FunctionIdentifier SPATIAL_INTERSECT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-intersect", 2);
    public final static FunctionIdentifier SPATIAL_AREA = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-area", 1);
    public final static FunctionIdentifier SPATIAL_DISTANCE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-distance", 2);
    public final static FunctionIdentifier CREATE_MBR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "create-mbr", 3);
    public final static FunctionIdentifier SPATIAL_CELL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "spatial-cell", 4);
    public final static FunctionIdentifier SWITCH_CASE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "switch-case", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier REG_EXP = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "reg-exp", 2);

    public final static FunctionIdentifier INJECT_FAILURE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "inject-failure", 2);
    public final static FunctionIdentifier CAST_RECORD = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "cast-record", 1);
    public final static FunctionIdentifier CAST_LIST = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "cast-list", 1);

    // Spatial and temporal type accessors
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_YEAR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-year", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MONTH = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-month", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_DAY = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-day", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_HOUR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-hour", 1);
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
    public static final FunctionIdentifier INTERVAL_BIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "interval-bin", 3);

    // Temporal functions
    public static final FunctionIdentifier DATE_FROM_UNIX_TIME_IN_DAYS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "date-from-unix-time-in-days", 1);
    public static final FunctionIdentifier DATE_FROM_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-date-from-datetime", 1);
    public final static FunctionIdentifier TIME_FROM_UNIX_TIME_IN_MS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "time-from-unix-time-in-ms", 1);
    public final static FunctionIdentifier TIME_FROM_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "get-time-from-datetime", 1);
    public final static FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_MS = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "datetime-from-unix-time-in-ms", 1);
    public final static FunctionIdentifier DATETIME_FROM_DATE_TIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "datetime-from-date-time", 2);
    public final static FunctionIdentifier CALENDAR_DURATION_FROM_DATETIME = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "calendar-duration-from-datetime", 2);
    public final static FunctionIdentifier CALENDAR_DURATION_FROM_DATE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "calendar-duration-from-date", 2);
    public final static FunctionIdentifier ADJUST_TIME_FOR_TIMEZONE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "adjust-time-for-timezone", 2);
    public final static FunctionIdentifier ADJUST_DATETIME_FOR_TIMEZONE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "adjust-datetime-for-timezone", 2);
    public final static FunctionIdentifier DAY_OF_WEEK = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "day-of-week");
    public final static FunctionIdentifier PARSE_DATE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-date", 2);
    public final static FunctionIdentifier PARSE_TIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-time", 2);
    public final static FunctionIdentifier PARSE_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "parse-datetime", 2);
    public final static FunctionIdentifier PRINT_DATE = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-date", 2);
    public final static FunctionIdentifier PRINT_TIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-time", 2);
    public final static FunctionIdentifier PRINT_DATETIME = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "print-datetime", 2);

    public final static FunctionIdentifier GET_POINT_X_COORDINATE_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-x", 1);
    public final static FunctionIdentifier GET_POINT_Y_COORDINATE_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-y", 1);
    public final static FunctionIdentifier GET_CIRCLE_RADIUS_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-radius", 1);
    public final static FunctionIdentifier GET_CIRCLE_CENTER_ACCESSOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "get-center", 1);
    public final static FunctionIdentifier GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR = new FunctionIdentifier(
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
    public static final FunctionIdentifier IS_NULL = AlgebricksBuiltinFunctions.IS_NULL;

    public static final FunctionIdentifier NOT_NULL = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "not-null",
            1);
    public static final FunctionIdentifier COLLECTION_TO_SEQUENCE = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "" + "collection-to-sequence", 1);

    public static IFunctionInfo getAsterixFunctionInfo(FunctionIdentifier fid) {
        IFunctionInfo finfo = registeredFunctions.get(fid);
        if (finfo == null) {
            finfo = new AsterixFunctionInfo(fid);
        }
        return finfo;
    }

    public static AsterixFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return (AsterixFunctionInfo) registeredFunctions.get(fid);
    }

    static {

        // first, take care of Algebricks builtin functions
        addFunction(IS_NULL, ABooleanTypeComputer.INSTANCE);
        addFunction(NOT, UnaryBooleanOrNullFunctionTypeComputer.INSTANCE);

        addPrivateFunction(EQ, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(LE, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(GE, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(LT, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(GT, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(AND, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(NEQ, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(OR, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_ADD, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);

        // and then, Asterix builtin functions
        addPrivateFunction(NOT_NULL, NotNullTypeComputer.INSTANCE);
        addPrivateFunction(ANY_COLLECTION_MEMBER, NonTaggedCollectionMemberResultType.INSTANCE);
        addFunction(AVG, OptionalADoubleTypeComputer.INSTANCE);
        addFunction(BOOLEAN_CONSTRUCTOR, UnaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(CARET, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        addFunction(CIRCLE_CONSTRUCTOR, OptionalACircleTypeComputer.INSTANCE);
        addPrivateFunction(CLOSED_RECORD_CONSTRUCTOR, ClosedRecordConstructorResultType.INSTANCE);
        addPrivateFunction(CONCAT_NON_NULL, ConcatNonNullTypeComputer.INSTANCE);

        addFunction(CONTAINS, ABooleanTypeComputer.INSTANCE);
        addFunction(COUNT, AInt64TypeComputer.INSTANCE);
        addPrivateFunction(COUNTHASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        addPrivateFunction(COUNTHASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        addFunction(CREATE_CIRCLE, ACircleTypeComputer.INSTANCE);
        addFunction(CREATE_LINE, ALineTypeComputer.INSTANCE);
        addPrivateFunction(CREATE_MBR, ADoubleTypeComputer.INSTANCE);
        addFunction(CREATE_POINT, APointTypeComputer.INSTANCE);
        addFunction(CREATE_POLYGON, APolygonTypeComputer.INSTANCE);
        addFunction(CREATE_RECTANGLE, ARectangleTypeComputer.INSTANCE);

        addFunction(DATE_CONSTRUCTOR, OptionalADateTypeComputer.INSTANCE);
        addFunction(DATETIME_CONSTRUCTOR, OptionalADateTimeTypeComputer.INSTANCE);
        addFunction(DOUBLE_CONSTRUCTOR, OptionalADoubleTypeComputer.INSTANCE);
        addFunction(DURATION_CONSTRUCTOR, OptionalADurationTypeComputer.INSTANCE);
        addFunction(YEAR_MONTH_DURATION_CONSTRUCTOR, OptionalAYearMonthDurationTypeComputer.INSTANCE);
        addFunction(DAY_TIME_DURATION_CONSTRUCTOR, OptionalADayTimeDurationTypeComputer.INSTANCE);
        addFunction(EDIT_DISTANCE, AInt32TypeComputer.INSTANCE);
        addFunction(EDIT_DISTANCE_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        addPrivateFunction(EDIT_DISTANCE_STRING_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE);
        addPrivateFunction(EDIT_DISTANCE_LIST_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE);
        addPrivateFunction(EMBED_TYPE, new IResultTypeComputer() {
            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return (IAType) BuiltinType.ANY;
            }
        });
        addPrivateFunction(EMPTY_STREAM, ABooleanTypeComputer.INSTANCE);
        addFunction(ENDS_WITH, ABooleanTypeComputer.INSTANCE);
        // add(FIELD_ACCESS, NonTaggedFieldAccessByNameResultType.INSTANCE);
        addPrivateFunction(FIELD_ACCESS_BY_INDEX, FieldAccessByIndexResultType.INSTANCE);
        addPrivateFunction(FIELD_ACCESS_BY_NAME, NonTaggedFieldAccessByNameResultType.INSTANCE);
        addFunction(FLOAT_CONSTRUCTOR, OptionalAFloatTypeComputer.INSTANCE);
        addPrivateFunction(FUZZY_EQ, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addPrivateFunction(GET_HANDLE, null); // TODO
        addPrivateFunction(GET_ITEM, NonTaggedGetItemResultType.INSTANCE);
        addPrivateFunction(GET_DATA, null); // TODO
        addPrivateFunction(GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        addPrivateFunction(GRAM_TOKENS, OrderedListOfAStringTypeComputer.INSTANCE);
        addFunction(GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        addPrivateFunction(HASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        addPrivateFunction(HASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        addPrivateFunction(INDEX_SEARCH, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return BuiltinType.ANY; // TODO
            }
        });
        addFunction(INT8_CONSTRUCTOR, OptionalAInt8TypeComputer.INSTANCE);
        addFunction(INT16_CONSTRUCTOR, OptionalAInt16TypeComputer.INSTANCE);
        addFunction(INT32_CONSTRUCTOR, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(INT64_CONSTRUCTOR, OptionalAInt64TypeComputer.INSTANCE);
        addFunction(LEN, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(LIKE, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        addFunction(LINE_CONSTRUCTOR, OptionalALineTypeComputer.INSTANCE);
        addPrivateFunction(LISTIFY, OrderedListConstructorResultType.INSTANCE);
        addPrivateFunction(LOCAL_AVG, NonTaggedLocalAvgTypeComputer.INSTANCE);
        addPrivateFunction(MAKE_FIELD_INDEX_HANDLE, null); // TODO
        addPrivateFunction(MAKE_FIELD_NAME_HANDLE, null); // TODO
        addFunction(MAX, NonTaggedMinMaxAggTypeComputer.INSTANCE);
        addPrivateFunction(LOCAL_MAX, NonTaggedMinMaxAggTypeComputer.INSTANCE);
        addFunction(MIN, NonTaggedMinMaxAggTypeComputer.INSTANCE);
        addPrivateFunction(LOCAL_MIN, NonTaggedMinMaxAggTypeComputer.INSTANCE);
        addPrivateFunction(NON_EMPTY_STREAM, ABooleanTypeComputer.INSTANCE);
        addFunction(NULL_CONSTRUCTOR, ANullTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_UNARY_MINUS, NonTaggedUnaryMinusTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_SUBTRACT, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_MULTIPLY, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_DIVIDE, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_MOD, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        addPrivateFunction(NUMERIC_IDIV, AInt32TypeComputer.INSTANCE);

        addFunction(NUMERIC_ABS, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        addFunction(NUMERIC_CEILING, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        addFunction(NUMERIC_FLOOR, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        addFunction(NUMERIC_ROUND, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        addFunction(NUMERIC_ROUND_HALF_TO_EVEN2, NonTaggedNumericRoundHalfToEven2TypeComputer.INSTANCE);

        addFunction(STRING_TO_CODEPOINT, OrderedListOfAInt32TypeComputer.INSTANCE);
        addFunction(CODEPOINT_TO_STRING, AStringTypeComputer.INSTANCE);
        addFunction(STRING_CONCAT, OptionalAStringTypeComputer.INSTANCE);
        addFunction(SUBSTRING2, Substring2TypeComputer.INSTANCE);
        addFunction(STRING_LENGTH, UnaryStringInt32OrNullTypeComputer.INSTANCE);
        addFunction(STRING_LOWERCASE, UnaryStringOrNullTypeComputer.INSTANCE);
        addFunction(STRING_START_WITH, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        addFunction(STRING_END_WITH, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        addFunction(STRING_MATCHES, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        addFunction(STRING_MATCHES_WITH_FLAG, TripleStringBoolOrNullTypeComputer.INSTANCE);
        addFunction(STRING_REPLACE, TripleStringStringOrNullTypeComputer.INSTANCE);
        addFunction(STRING_REPLACE_WITH_FLAG, QuadStringStringOrNullTypeComputer.INSTANCE);
        addFunction(SUBSTRING_BEFORE, BinaryStringStringOrNullTypeComputer.INSTANCE);
        addFunction(SUBSTRING_AFTER, BinaryStringStringOrNullTypeComputer.INSTANCE);
        addPrivateFunction(STRING_EQUAL, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        addFunction(STRING_JOIN, AStringTypeComputer.INSTANCE);

        addPrivateFunction(OPEN_RECORD_CONSTRUCTOR, OpenRecordConstructorResultType.INSTANCE);
        addPrivateFunction(ORDERED_LIST_CONSTRUCTOR, OrderedListConstructorResultType.INSTANCE);
        addFunction(POINT_CONSTRUCTOR, OptionalAPointTypeComputer.INSTANCE);
        addFunction(POINT3D_CONSTRUCTOR, OptionalAPoint3DTypeComputer.INSTANCE);
        addFunction(POLYGON_CONSTRUCTOR, OptionalAPolygonTypeComputer.INSTANCE);
        addPrivateFunction(PREFIX_LEN_JACCARD, AInt32TypeComputer.INSTANCE);
        addFunction(RANGE, AInt32TypeComputer.INSTANCE);
        addFunction(RECTANGLE_CONSTRUCTOR, OptionalARectangleTypeComputer.INSTANCE);

        addFunction(SCALAR_AVG, ScalarVersionOfAggregateResultType.INSTANCE);
        addFunction(SCALAR_COUNT, AInt64TypeComputer.INSTANCE);
        addPrivateFunction(SCALAR_GLOBAL_AVG, ScalarVersionOfAggregateResultType.INSTANCE);
        addPrivateFunction(SCALAR_LOCAL_AVG, ScalarVersionOfAggregateResultType.INSTANCE);
        addFunction(SCALAR_MAX, ScalarVersionOfAggregateResultType.INSTANCE);
        addFunction(SCALAR_MIN, ScalarVersionOfAggregateResultType.INSTANCE);
        addFunction(SCALAR_SUM, ScalarVersionOfAggregateResultType.INSTANCE);
        addPrivateFunction(SCAN_COLLECTION, NonTaggedCollectionMemberResultType.INSTANCE);
        addPrivateFunction(SERIAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        addPrivateFunction(SERIAL_COUNT, AInt64TypeComputer.INSTANCE);
        addPrivateFunction(SERIAL_GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        addPrivateFunction(SERIAL_LOCAL_AVG, NonTaggedLocalAvgTypeComputer.INSTANCE);
        addPrivateFunction(SERIAL_SUM, NonTaggedNumericAggTypeComputer.INSTANCE);
        addPrivateFunction(SERIAL_LOCAL_SUM, NonTaggedNumericAggTypeComputer.INSTANCE);
        addFunction(SIMILARITY_JACCARD, AFloatTypeComputer.INSTANCE);
        addFunction(SIMILARITY_JACCARD_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        addPrivateFunction(SIMILARITY_JACCARD_SORTED, AFloatTypeComputer.INSTANCE);
        addPrivateFunction(SIMILARITY_JACCARD_SORTED_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        addPrivateFunction(SIMILARITY_JACCARD_PREFIX, AFloatTypeComputer.INSTANCE);
        addPrivateFunction(SIMILARITY_JACCARD_PREFIX_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        addFunction(SPATIAL_AREA, ADoubleTypeComputer.INSTANCE);
        addFunction(SPATIAL_CELL, ARectangleTypeComputer.INSTANCE);
        addFunction(SPATIAL_DISTANCE, ADoubleTypeComputer.INSTANCE);
        addFunction(SPATIAL_INTERSECT, ABooleanTypeComputer.INSTANCE);
        addFunction(GET_POINT_X_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE);
        addFunction(GET_POINT_Y_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE);
        addFunction(GET_CIRCLE_RADIUS_ACCESSOR, ADoubleTypeComputer.INSTANCE);
        addFunction(GET_CIRCLE_CENTER_ACCESSOR, APointTypeComputer.INSTANCE);
        addFunction(GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR, OrderedListOfAPointTypeComputer.INSTANCE);
        addFunction(STARTS_WITH, ABooleanTypeComputer.INSTANCE);
        addFunction(STRING_CONSTRUCTOR, OptionalAStringTypeComputer.INSTANCE);
        addPrivateFunction(SUBSET_COLLECTION, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) expression;
                IAType t;
                try {
                    t = (IAType) env.getType(fun.getArguments().get(0).getValue());
                } catch (AlgebricksException e) {
                    throw new AlgebricksException(e);
                }
                switch (t.getTypeTag()) {
                    case UNORDEREDLIST:
                    case ORDEREDLIST: {
                        AbstractCollectionType act = (AbstractCollectionType) t;
                        return act.getItemType();
                    }
                    case UNION: {
                        AUnionType ut = (AUnionType) t;
                        if (!ut.isNullableType()) {
                            throw new AlgebricksException("Expecting collection type. Found " + t);
                        }
                        IAType t2 = ut.getUnionList().get(1);
                        ATypeTag tag2 = t2.getTypeTag();
                        if (tag2 == ATypeTag.UNORDEREDLIST || tag2 == ATypeTag.ORDEREDLIST) {
                            AbstractCollectionType act = (AbstractCollectionType) t2;
                            return act.getItemType();
                        }
                        throw new AlgebricksException("Expecting collection type. Found " + t);
                    }
                    default: {
                        throw new AlgebricksException("Expecting collection type. Found " + t);
                    }
                }
            }
        });
        addFunction(SUBSTRING, SubstringTypeComputer.INSTANCE);
        addFunction(SUM, NonTaggedNumericAggTypeComputer.INSTANCE);
        addPrivateFunction(LOCAL_SUM, NonTaggedNumericAggTypeComputer.INSTANCE);
        addFunction(SWITCH_CASE, NonTaggedSwitchCaseComputer.INSTANCE);
        addPrivateFunction(REG_EXP, ABooleanTypeComputer.INSTANCE);
        addFunction(INJECT_FAILURE, InjectFailureTypeComputer.INSTANCE);
        addPrivateFunction(CAST_RECORD, CastRecordResultTypeComputer.INSTANCE);
        addFunction(CAST_LIST, CastListResultTypeComputer.INSTANCE);

        addFunction(TID, AInt32TypeComputer.INSTANCE);
        addFunction(TIME_CONSTRUCTOR, OptionalATimeTypeComputer.INSTANCE);
        addPrivateFunction(TYPE_OF, null);
        addPrivateFunction(UNORDERED_LIST_CONSTRUCTOR, UnorderedListConstructorResultType.INSTANCE);
        addFunction(WORD_TOKENS, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return new AOrderedListType(BuiltinType.ASTRING, "string");
            }
        });

        // temporal type accessors
        addFunction(ACCESSOR_TEMPORAL_YEAR, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_MONTH, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_DAY, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_HOUR, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_MIN, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_SEC, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_MILLISEC, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_START, OptionalATemporalInstanceTypeComputer.INSTANCE);
        addFunction(ACCESSOR_TEMPORAL_INTERVAL_END, OptionalATemporalInstanceTypeComputer.INSTANCE);

        // temporal functions
        addFunction(DATE_FROM_UNIX_TIME_IN_DAYS, OptionalADateTypeComputer.INSTANCE);
        addFunction(DATE_FROM_DATETIME, OptionalADateTypeComputer.INSTANCE);
        addFunction(TIME_FROM_UNIX_TIME_IN_MS, OptionalATimeTypeComputer.INSTANCE);
        addFunction(TIME_FROM_DATETIME, OptionalATimeTypeComputer.INSTANCE);
        addFunction(DATETIME_FROM_DATE_TIME, OptionalADateTimeTypeComputer.INSTANCE);
        addFunction(DATETIME_FROM_UNIX_TIME_IN_MS, OptionalADateTimeTypeComputer.INSTANCE);
        addFunction(CALENDAR_DURATION_FROM_DATETIME, OptionalADurationTypeComputer.INSTANCE);
        addFunction(CALENDAR_DURATION_FROM_DATE, OptionalADurationTypeComputer.INSTANCE);
        addFunction(ADJUST_DATETIME_FOR_TIMEZONE, OptionalAStringTypeComputer.INSTANCE);
        addFunction(ADJUST_TIME_FOR_TIMEZONE, OptionalAStringTypeComputer.INSTANCE);
        addFunction(INTERVAL_BEFORE, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_AFTER, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_MEETS, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_MET_BY, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_OVERLAPS, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_OVERLAPPED_BY, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(OVERLAP, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_STARTS, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_STARTED_BY, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_COVERS, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_COVERED_BY, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_ENDS, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(INTERVAL_ENDED_BY, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(CURRENT_DATE, ADateTypeComputer.INSTANCE);
        addFunction(CURRENT_TIME, ATimeTypeComputer.INSTANCE);
        addFunction(CURRENT_DATETIME, ADateTimeTypeComputer.INSTANCE);
        addFunction(DAY_TIME_DURATION_GREATER_THAN, OptionalABooleanTypeComputer.INSTANCE);
        addPrivateFunction(DAY_TIME_DURATION_LESS_THAN, OptionalABooleanTypeComputer.INSTANCE);
        addPrivateFunction(YEAR_MONTH_DURATION_GREATER_THAN, OptionalABooleanTypeComputer.INSTANCE);
        addPrivateFunction(YEAR_MONTH_DURATION_LESS_THAN, OptionalABooleanTypeComputer.INSTANCE);
        addPrivateFunction(DURATION_EQUAL, OptionalABooleanTypeComputer.INSTANCE);
        addFunction(DURATION_FROM_MONTHS, OptionalADurationTypeComputer.INSTANCE);
        addFunction(DURATION_FROM_MILLISECONDS, OptionalADurationTypeComputer.INSTANCE);
        addFunction(MONTHS_FROM_YEAR_MONTH_DURATION, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(MILLISECONDS_FROM_DAY_TIME_DURATION, OptionalAInt64TypeComputer.INSTANCE);
        addFunction(GET_DAY_TIME_DURATION, OptionalADayTimeDurationTypeComputer.INSTANCE);
        addFunction(GET_YEAR_MONTH_DURATION, OptionalAYearMonthDurationTypeComputer.INSTANCE);
        addFunction(INTERVAL_BIN, OptionalAIntervalTypeComputer.INSTANCE);
        addFunction(DAY_OF_WEEK, OptionalAInt32TypeComputer.INSTANCE);
        addFunction(PARSE_DATE, OptionalADateTypeComputer.INSTANCE);
        addFunction(PARSE_TIME, OptionalATimeTypeComputer.INSTANCE);
        addFunction(PARSE_DATETIME, OptionalADateTimeTypeComputer.INSTANCE);
        addFunction(PRINT_DATE, OptionalAStringTypeComputer.INSTANCE);
        addFunction(PRINT_TIME, OptionalAStringTypeComputer.INSTANCE);
        addFunction(PRINT_DATETIME, OptionalAStringTypeComputer.INSTANCE);

        // interval constructors
        addFunction(INTERVAL_CONSTRUCTOR_DATE, OptionalAIntervalTypeComputer.INSTANCE);
        addFunction(INTERVAL_CONSTRUCTOR_TIME, OptionalAIntervalTypeComputer.INSTANCE);
        addFunction(INTERVAL_CONSTRUCTOR_DATETIME, OptionalAIntervalTypeComputer.INSTANCE);
        addFunction(INTERVAL_CONSTRUCTOR_START_FROM_DATE, OptionalAIntervalTypeComputer.INSTANCE);
        addFunction(INTERVAL_CONSTRUCTOR_START_FROM_DATETIME, OptionalAIntervalTypeComputer.INSTANCE);
        addFunction(INTERVAL_CONSTRUCTOR_START_FROM_TIME, OptionalAIntervalTypeComputer.INSTANCE);

        addPrivateFunction(COLLECTION_TO_SEQUENCE, CollectionToSequenceTypeComputer.INSTANCE);

        String metadataFunctionLoaderClassName = "edu.uci.ics.asterix.metadata.functions.MetadataBuiltinFunctions";
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
    }

    static {
        addAgg(AVG);
        addAgg(LOCAL_AVG);
        addAgg(GLOBAL_AVG);
        addLocalAgg(AVG, LOCAL_AVG);
        addGlobalAgg(AVG, GLOBAL_AVG);

        addAgg(COUNT);
        addLocalAgg(COUNT, COUNT);
        addGlobalAgg(COUNT, SUM);

        addAgg(MAX);
        addAgg(LOCAL_MAX);
        addLocalAgg(MAX, LOCAL_MAX);
        addGlobalAgg(MAX, MAX);

        addAgg(MIN);
        addLocalAgg(MIN, LOCAL_MIN);
        addGlobalAgg(MIN, MIN);

        addAgg(SUM);
        addAgg(LOCAL_SUM);
        addLocalAgg(SUM, LOCAL_SUM);
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
        addGlobalAgg(SERIAL_COUNT, SERIAL_SUM);

        addAgg(SERIAL_AVG);
        addAgg(SERIAL_LOCAL_AVG);
        addAgg(SERIAL_GLOBAL_AVG);
        addLocalAgg(SERIAL_AVG, SERIAL_LOCAL_AVG);
        addGlobalAgg(SERIAL_AVG, SERIAL_GLOBAL_AVG);

        addAgg(SERIAL_SUM);
        addAgg(SERIAL_LOCAL_SUM);
        addLocalAgg(SERIAL_SUM, SERIAL_LOCAL_SUM);
        addGlobalAgg(SERIAL_SUM, SERIAL_SUM);
    }

    static {
        datasetFunctions.add(getAsterixFunctionInfo(DATASET));
        datasetFunctions.add(getAsterixFunctionInfo(FEED_INGEST));
        datasetFunctions.add(getAsterixFunctionInfo(INDEX_SEARCH));
    }

    static {
        addUnnestFun(DATASET, false);
        addUnnestFun(FEED_INGEST, false);
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

    public static boolean isBuiltinCompilerFunction(FunctionIdentifier fi, boolean includePrivateFunctions) {
        return builtinPublicFunctionsSet.keySet().contains(getAsterixFunctionInfo(fi));
    }

    public static boolean isBuiltinCompilerFunction(FunctionSignature signature, boolean includePrivateFunctions) {

        FunctionIdentifier fi = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, signature.getName(),
                signature.getArity());
        IFunctionInfo finfo = getAsterixFunctionInfo(fi);
        if (builtinPublicFunctionsSet.keySet().contains(finfo)
                || (includePrivateFunctions && builtinPrivateFunctionsSet.keySet().contains(finfo))) {
            return true;
        }
        fi = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, signature.getName(), signature.getArity());
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
        if (ruv != null && ruv.booleanValue()) {
            return true;
        } else {
            return false;
        }
    }

    public static FunctionIdentifier getLocalAggregateFunction(FunctionIdentifier fi) {
        return aggregateToLocalAggregate.get(getAsterixFunctionInfo(fi)).getFunctionIdentifier();
    }

    public static FunctionIdentifier getGlobalAggregateFunction(FunctionIdentifier fi) {
        return aggregateToGlobalAggregate.get(getAsterixFunctionInfo(fi)).getFunctionIdentifier();
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
        if (serializableFinfo == null)
            throw new IllegalStateException("no serializable implementation for aggregate function "
                    + serializableFinfo);

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

    public static void addFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer) {
        IFunctionInfo functionInfo = getAsterixFunctionInfo(fi);
        builtinPublicFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        registeredFunctions.put(fi);
    }

    public static void addPrivateFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer) {
        IFunctionInfo functionInfo = getAsterixFunctionInfo(fi);
        builtinPrivateFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        registeredFunctions.put(fi);
    }

    private static void addAgg(FunctionIdentifier fi) {
        builtinAggregateFunctions.add(getAsterixFunctionInfo(fi));
    }

    private static void addLocalAgg(FunctionIdentifier fi, FunctionIdentifier localfi) {
        aggregateToLocalAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(localfi));
    }

    private static void addGlobalAgg(FunctionIdentifier fi, FunctionIdentifier globalfi) {
        aggregateToGlobalAggregate.put(getAsterixFunctionInfo(fi), getAsterixFunctionInfo(globalfi));
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

    public static boolean isSpatialFilterFunction(FunctionIdentifier fi) {
        return spatialFilterFunctions.get(getAsterixFunctionInfo(fi)) != null;
    }
    
    static {
        similarityFunctions.add(getAsterixFunctionInfo(SIMILARITY_JACCARD));
        similarityFunctions.add(getAsterixFunctionInfo(SIMILARITY_JACCARD_CHECK));
        similarityFunctions.add(getAsterixFunctionInfo(EDIT_DISTANCE));
        similarityFunctions.add(getAsterixFunctionInfo(EDIT_DISTANCE_CHECK));
    }

    public static boolean isSimilarityFunction(FunctionIdentifier fi) {
        return similarityFunctions.contains(getAsterixFunctionInfo(fi));
    }

}
