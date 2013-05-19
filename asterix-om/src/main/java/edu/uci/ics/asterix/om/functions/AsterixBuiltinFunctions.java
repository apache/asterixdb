package edu.uci.ics.asterix.om.functions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ACircleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADateTimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADateTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADoubleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AFloatTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AInt32TypeComputer;
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
import edu.uci.ics.asterix.om.typecomputer.impl.FieldAccessByIndexResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.InjectFailureTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedCollectionMemberResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedFieldAccessByNameResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedGetItemResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedLocalAvgTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericAddSubMulDivTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericRoundHalfToEven2TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericUnaryFunctionTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedSumTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedSwitchCaseComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedUnaryMinusTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NotNullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OpenRecordConstructorResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalABooleanTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalACircleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADateTimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalADateTypeComputer;
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
import edu.uci.ics.asterix.om.types.TypeHelper;
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

    /*
     * A function is categorized as public or private depending upon whether it can be used by the end-user in AQL queries.
     */
    public enum FunctionNamespace {
        ASTERIX_PUBLIC,
        ASTERIX_PRIVATE
    }

    private static final FunctionInfoRepository finfoRepo = new FunctionInfoRepository();

    // it is supposed to be an identity mapping
    private final static Map<IFunctionInfo, IFunctionInfo> builtinFunctionsSet = new HashMap<IFunctionInfo, IFunctionInfo>();
    private final static Map<IFunctionInfo, IResultTypeComputer> funTypeComputer = new HashMap<IFunctionInfo, IResultTypeComputer>();

    private final static Set<IFunctionInfo> builtinAggregateFunctions = new HashSet<IFunctionInfo>();
    private static final Set<IFunctionInfo> datasetFunctions = new HashSet<IFunctionInfo>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToLocalAggregate = new HashMap<IFunctionInfo, IFunctionInfo>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToGlobalAggregate = new HashMap<IFunctionInfo, IFunctionInfo>();
    private static final Map<IFunctionInfo, IFunctionInfo> aggregateToSerializableAggregate = new HashMap<IFunctionInfo, IFunctionInfo>();
    private final static Map<IFunctionInfo, Boolean> builtinUnnestingFunctions = new HashMap<IFunctionInfo, Boolean>();
    private final static Map<IFunctionInfo, IFunctionInfo> scalarToAggregateFunctionMap = new HashMap<IFunctionInfo, IFunctionInfo>();
    private static final Map<IFunctionInfo, SpatialFilterKind> spatialFilterFunctions = new HashMap<IFunctionInfo, SpatialFilterKind>();

    public final static FunctionIdentifier TYPE_OF = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "type-of", 1);
    public final static FunctionIdentifier GET_HANDLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "get-handle", 2);
    public final static FunctionIdentifier GET_DATA = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "get-data", 2);
    public final static FunctionIdentifier EMBED_TYPE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "embed-type", 1);

    public final static FunctionIdentifier GET_ITEM = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "get-item", 2);
    public final static FunctionIdentifier ANY_COLLECTION_MEMBER = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "any-collection-member", 1);
    public final static FunctionIdentifier LISTIFY = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "listify", 1);
    // public final static FunctionIdentifier BAGIFY = new
    // FunctionIdentifier(ASTERIX_NS, "bagify", 1, true);
    public final static FunctionIdentifier LEN = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(), "len",
            1);

    public final static FunctionIdentifier CONCAT_NON_NULL = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "concat-non-null", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier EMPTY_STREAM = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "empty-stream", 0);
    public final static FunctionIdentifier NON_EMPTY_STREAM = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "non-empty-stream", 0);
    public final static FunctionIdentifier ORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "ordered-list-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier UNORDERED_LIST_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "unordered-list-constructor", FunctionIdentifier.VARARGS);

    // records
    public final static FunctionIdentifier CLOSED_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "closed-record-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier OPEN_RECORD_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "open-record-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier RECORD_TYPE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "record-type-constructor", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier FIELD_ACCESS_BY_INDEX = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "field-access-by-index", 2);
    public final static FunctionIdentifier FIELD_ACCESS_BY_NAME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "field-access-by-name", 2);

    public final static FunctionIdentifier NUMERIC_UNARY_MINUS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-unary-minus", 1);

    public final static FunctionIdentifier NUMERIC_SUBTRACT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-subtract", 2);
    public final static FunctionIdentifier NUMERIC_MULTIPLY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-multiply", 2);
    public final static FunctionIdentifier NUMERIC_DIVIDE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-divide", 2);
    public final static FunctionIdentifier NUMERIC_MOD = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-mod", 2);
    public final static FunctionIdentifier NUMERIC_IDIV = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-idiv", 2);
    public final static FunctionIdentifier CARET = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "caret", 2);

    public final static FunctionIdentifier NUMERIC_ABS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-abs", 1);
    public final static FunctionIdentifier NUMERIC_CEILING = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-ceiling", 1);
    public final static FunctionIdentifier NUMERIC_FLOOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-floor", 1);
    public final static FunctionIdentifier NUMERIC_ROUND = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-round", 1);
    public final static FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-round-half-to-even", 1);
    public final static FunctionIdentifier NUMERIC_ROUND_HALF_TO_EVEN2 = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "numeric-round-half-to-even", 2);
    // String funcitons
    public final static FunctionIdentifier STRING_EQUAL = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "string-equal", 2);
    public final static FunctionIdentifier STRING_START_WITH = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "start-with", 2);
    public final static FunctionIdentifier STRING_END_WITH = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "end-with", 2);
    public final static FunctionIdentifier STRING_MATCHES = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "matches", 2);
    public final static FunctionIdentifier STRING_MATCHES_WITH_FLAG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "matches", 3);
    public final static FunctionIdentifier STRING_LOWERCASE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "lowercase", 1);
    public final static FunctionIdentifier STRING_REPLACE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "replace", 3);
    public final static FunctionIdentifier STRING_REPLACE_WITH_FLAG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "replace", 4);
    public final static FunctionIdentifier STRING_LENGTH = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "string-length", 1);
    public final static FunctionIdentifier SUBSTRING2 = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "substring", 2);
    public final static FunctionIdentifier SUBSTRING_BEFORE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "substring-before", 2);
    public final static FunctionIdentifier SUBSTRING_AFTER = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "substring-after", 2);
    public final static FunctionIdentifier STRING_TO_CODEPOINT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "string-to-codepoint", 1);
    public final static FunctionIdentifier CODEPOINT_TO_STRING = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "codepoint-to-string", 1);
    public final static FunctionIdentifier STRING_CONCAT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "string-concat", 1);
    public final static FunctionIdentifier STRING_JOIN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "string-join", 2);

    public final static FunctionIdentifier DATASET = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "dataset", 1);
    public final static FunctionIdentifier FEED_INGEST = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "feed-ingest", 1);

    public final static FunctionIdentifier INDEX_SEARCH = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "index-search", FunctionIdentifier.VARARGS);

    public final static FunctionIdentifier MAKE_FIELD_INDEX_HANDLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "make-field-index-handle", 2);
    public final static FunctionIdentifier MAKE_FIELD_NAME_HANDLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "make-field-name-handle", 1);

    public final static FunctionIdentifier SUBSTRING = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "substring", 3);
    public final static FunctionIdentifier LIKE = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "like", 2);
    public final static FunctionIdentifier CONTAINS = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "contains", 2);
    public final static FunctionIdentifier STARTS_WITH = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "starts-with", 2);
    public final static FunctionIdentifier ENDS_WITH = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "ends-with", 2);

    public final static FunctionIdentifier AVG = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-avg", 1);
    public final static FunctionIdentifier COUNT = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-count", 1);
    public final static FunctionIdentifier SUM = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-sum", 1);
    public final static FunctionIdentifier LOCAL_SUM = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-local-sum", 1);
    public final static FunctionIdentifier MAX = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-max", 1);
    public final static FunctionIdentifier LOCAL_MAX = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-local-max", 1);
    public final static FunctionIdentifier MIN = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-min", 1);
    public final static FunctionIdentifier LOCAL_MIN = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-local-min", 1);
    public final static FunctionIdentifier GLOBAL_AVG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "agg-global-avg", 1);
    public final static FunctionIdentifier LOCAL_AVG = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "agg-local-avg", 1);

    public final static FunctionIdentifier SCALAR_AVG = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "avg", 1);
    public final static FunctionIdentifier SCALAR_COUNT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "count", 1);
    public final static FunctionIdentifier SCALAR_SUM = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "sum", 1);
    public final static FunctionIdentifier SCALAR_MAX = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "max", 1);
    public final static FunctionIdentifier SCALAR_MIN = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "min", 1);
    public final static FunctionIdentifier SCALAR_GLOBAL_AVG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "global-avg", 1);
    public final static FunctionIdentifier SCALAR_LOCAL_AVG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "local-avg", 1);

    // serializable aggregate functions
    public final static FunctionIdentifier SERIAL_AVG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "avg-serial", 1);
    public final static FunctionIdentifier SERIAL_COUNT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "count-serial", 1);
    public final static FunctionIdentifier SERIAL_SUM = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "sum-serial", 1);
    public final static FunctionIdentifier SERIAL_LOCAL_SUM = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "local-sum-serial", 1);
    public final static FunctionIdentifier SERIAL_GLOBAL_AVG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "global-avg-serial", 1);
    public final static FunctionIdentifier SERIAL_LOCAL_AVG = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "local-avg-serial", 1);

    public final static FunctionIdentifier SCAN_COLLECTION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "scan-collection", 1);
    public final static FunctionIdentifier SUBSET_COLLECTION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "subset-collection", 3);

    public final static FunctionIdentifier RANGE = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "range", 2);

    // fuzzy functions:
    public final static FunctionIdentifier FUZZY_EQ = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "fuzzy-eq", 2);

    public final static FunctionIdentifier PREFIX_LEN_JACCARD = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "prefix-len-jaccard", 2);

    public final static FunctionIdentifier SIMILARITY_JACCARD = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "similarity-jaccard", 2);
    public final static FunctionIdentifier SIMILARITY_JACCARD_CHECK = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "similarity-jaccard-check", 3);
    public final static FunctionIdentifier SIMILARITY_JACCARD_SORTED = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "similarity-jaccard-sorted", 2);
    public final static FunctionIdentifier SIMILARITY_JACCARD_SORTED_CHECK = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "similarity-jaccard-sorted-check", 3);
    public final static FunctionIdentifier SIMILARITY_JACCARD_PREFIX = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "similarity-jaccard-prefix", 6);
    public final static FunctionIdentifier SIMILARITY_JACCARD_PREFIX_CHECK = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "similarity-jaccard-prefix-check", 6);

    public final static FunctionIdentifier EDIT_DISTANCE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "edit-distance", 2);
    public final static FunctionIdentifier EDIT_DISTANCE_CHECK = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "edit-distance-check", 3);
    public final static FunctionIdentifier EDIT_DISTANCE_LIST_IS_FILTERABLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "edit-distance-list-is-filterable", 2);
    public final static FunctionIdentifier EDIT_DISTANCE_STRING_IS_FILTERABLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "edit-distance-string-is-filterable", 4);

    // tokenizers:
    public final static FunctionIdentifier WORD_TOKENS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "word-tokens", 1);
    public final static FunctionIdentifier HASHED_WORD_TOKENS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "hashed-word-tokens", 1);
    public final static FunctionIdentifier COUNTHASHED_WORD_TOKENS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "counthashed-word-tokens", 1);
    public final static FunctionIdentifier GRAM_TOKENS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "gram-tokens", 3);
    public final static FunctionIdentifier HASHED_GRAM_TOKENS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "hashed-gram-tokens", 3);
    public final static FunctionIdentifier COUNTHASHED_GRAM_TOKENS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "counthashed-gram-tokens", 3);

    public final static FunctionIdentifier TID = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "tid", 0);

    // constructors:
    public final static FunctionIdentifier BOOLEAN_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "boolean", 1);
    public final static FunctionIdentifier NULL_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "null", 1);
    public final static FunctionIdentifier STRING_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "string", 1);
    public final static FunctionIdentifier INT8_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "int8", 1);
    public final static FunctionIdentifier INT16_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "int16", 1);
    public final static FunctionIdentifier INT32_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "int32", 1);
    public final static FunctionIdentifier INT64_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "int64", 1);
    public final static FunctionIdentifier FLOAT_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "float", 1);
    public final static FunctionIdentifier DOUBLE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "double", 1);
    public final static FunctionIdentifier POINT_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "point", 1);
    public final static FunctionIdentifier POINT3D_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "point3d", 1);
    public final static FunctionIdentifier LINE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "line", 1);
    public final static FunctionIdentifier CIRCLE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "circle", 1);
    public final static FunctionIdentifier RECTANGLE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "rectangle", 1);
    public final static FunctionIdentifier POLYGON_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "polygon", 1);
    public final static FunctionIdentifier TIME_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "time", 1);
    public final static FunctionIdentifier DATE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "date", 1);
    public final static FunctionIdentifier DATETIME_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "datetime", 1);
    public final static FunctionIdentifier DURATION_CONSTRUCTOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "duration", 1);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_DATE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-from-date", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_TIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-from-time", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-from-datetime", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-start-from-date", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_TIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-start-from-time", 2);
    public final static FunctionIdentifier INTERVAL_CONSTRUCTOR_START_FROM_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-start-from-datetime", 2);
    public final static FunctionIdentifier INTERVAL_BEFORE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-before", 2);
    public final static FunctionIdentifier INTERVAL_AFTER = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-after", 2);
    public final static FunctionIdentifier INTERVAL_MEETS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-meets", 2);
    public final static FunctionIdentifier INTERVAL_MET_BY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-met-by", 2);
    public final static FunctionIdentifier INTERVAL_OVERLAPS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-overlaps", 2);
    public final static FunctionIdentifier INTERVAL_OVERLAPPED_BY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-overlapped-by", 2);
    public final static FunctionIdentifier OVERLAP = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "overlap", 2);
    public final static FunctionIdentifier INTERVAL_STARTS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-starts", 2);
    public final static FunctionIdentifier INTERVAL_STARTED_BY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-started-by", 2);
    public final static FunctionIdentifier INTERVAL_COVERS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-covers", 2);
    public final static FunctionIdentifier INTERVAL_COVERED_BY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-covered-by", 2);
    public final static FunctionIdentifier INTERVAL_ENDS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-ends", 2);
    public final static FunctionIdentifier INTERVAL_ENDED_BY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "interval-ended-by", 2);
    public final static FunctionIdentifier CURRENT_TIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "current-time", 0);
    public final static FunctionIdentifier CURRENT_DATE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "current-date", 0);
    public final static FunctionIdentifier CURRENT_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "current-datetime", 0);
    public final static FunctionIdentifier DURATION_EQUAL = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "duration-equal", 2);
    public final static FunctionIdentifier YEAR_MONTH_DURATION_GREATER_THAN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "year-month-duration-greater-than", 2);
    public final static FunctionIdentifier YEAR_MONTH_DURATION_LESS_THAN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "year-month-duration-less-than", 2);
    public final static FunctionIdentifier DAY_TIME_DURATION_GREATER_THAN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "day-time-duration-greater-than", 2);
    public final static FunctionIdentifier DAY_TIME_DURATION_LESS_THAN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "day-time-duration-less-than", 2);
    public final static FunctionIdentifier DURATION_FROM_MONTHS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "duration-from-months", 1);
    public final static FunctionIdentifier MONTHS_OF_YEAR_MONTH_DURATION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "months-of-year-month-duration", 1);
    public final static FunctionIdentifier DURATION_FROM_MILLISECONDS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "duration-from-ms", 1);
    public final static FunctionIdentifier MILLISECONDS_OF_DAY_TIME_DURATION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "ms-of-day-time-duration", 1);

    // spatial
    public final static FunctionIdentifier CREATE_POINT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "create-point", 2);
    public final static FunctionIdentifier CREATE_LINE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "create-line", 2);
    public final static FunctionIdentifier CREATE_POLYGON = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "create-polygon", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier CREATE_CIRCLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "create-circle", 2);
    public final static FunctionIdentifier CREATE_RECTANGLE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "create-rectangle", 2);
    public final static FunctionIdentifier SPATIAL_INTERSECT = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "spatial-intersect", 2);
    public final static FunctionIdentifier SPATIAL_AREA = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "spatial-area", 1);
    public final static FunctionIdentifier SPATIAL_DISTANCE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "spatial-distance", 2);
    public final static FunctionIdentifier CREATE_MBR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "create-mbr", 3);
    public final static FunctionIdentifier SPATIAL_CELL = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "spatial-cell", 4);
    public final static FunctionIdentifier SWITCH_CASE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "switch-case", FunctionIdentifier.VARARGS);
    public final static FunctionIdentifier REG_EXP = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "reg-exp", 2);

    public final static FunctionIdentifier INJECT_FAILURE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "inject-failure", 2);
    public final static FunctionIdentifier CAST_RECORD = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PRIVATE.name(), "cast-record", 1);
    public final static FunctionIdentifier CAST_LIST = new FunctionIdentifier(FunctionNamespace.ASTERIX_PUBLIC.name(),
            "cast-list", 1);

    // Spatial and temporal type accessors
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_YEAR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "year", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MONTH = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "month", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_DAY = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "day", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_HOUR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "hour", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MIN = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "minute", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_SEC = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "second", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_MILLISEC = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "millisecond", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_START = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-interval-start", 1);
    public static final FunctionIdentifier ACCESSOR_TEMPORAL_INTERVAL_END = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-interval-end", 1);

    // Temporal functions
    public static final FunctionIdentifier DATE_FROM_UNIX_TIME_IN_DAYS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "date-from-unix-time-in-days", 1);
    public static final FunctionIdentifier DATE_FROM_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "date-from-datetime", 1);
    public final static FunctionIdentifier ADD_DATE_DURATION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "add-date-duration", 2);
    public final static FunctionIdentifier SUBTRACT_DATE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "subtract-date", 2);
    public final static FunctionIdentifier TIME_FROM_UNIX_TIME_IN_MS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "time-from-unix-time-in-ms", 1);
    public final static FunctionIdentifier TIME_FROM_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "time-from-datetime", 1);
    public final static FunctionIdentifier SUBTRACT_TIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "subtract-time", 2);
    public final static FunctionIdentifier ADD_TIME_DURATION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "add-time-duration", 2);
    public final static FunctionIdentifier DATETIME_FROM_UNIX_TIME_IN_MS = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "datetime-from-unix-time-in-ms", 1);
    public final static FunctionIdentifier DATETIME_FROM_DATE_TIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "datetime-from-date-time", 2);
    public final static FunctionIdentifier SUBTRACT_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "subtract-datetime", 2);
    public final static FunctionIdentifier ADD_DATETIME_DURATION = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "add-datetime-duration", 2);
    public final static FunctionIdentifier CALENDAR_DURATION_FROM_DATETIME = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "calendar-duration-from-datetime", 2);
    public final static FunctionIdentifier CALENDAR_DURATION_FROM_DATE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "calendar-duration-from-date", 2);
    public final static FunctionIdentifier ADJUST_TIME_FOR_TIMEZONE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "adjust-time-for-timezone", 2);
    public final static FunctionIdentifier ADJUST_DATETIME_FOR_TIMEZONE = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "adjust-datetime-for-timezone", 2);

    public final static FunctionIdentifier GET_POINT_X_COORDINATE_ACCESSOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-x", 1);
    public final static FunctionIdentifier GET_POINT_Y_COORDINATE_ACCESSOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-y", 1);
    public final static FunctionIdentifier GET_CIRCLE_RADIUS_ACCESSOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-radius", 1);
    public final static FunctionIdentifier GET_CIRCLE_CENTER_ACCESSOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-center", 1);
    public final static FunctionIdentifier GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR = new FunctionIdentifier(
            FunctionNamespace.ASTERIX_PUBLIC.name(), "get-points", 1);

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

    public static final FunctionIdentifier NOT_NULL = new FunctionIdentifier(FunctionNamespace.ASTERIX_PRIVATE.name(),
            "not-null", 1);

    public static IFunctionInfo getAsterixFunctionInfo(FunctionIdentifier fid) {
        IFunctionInfo finfo = finfoRepo.get(fid);;
        if (finfo == null) {
            finfo = new AsterixFunctionInfo(fid);
        }
        return finfo;
    }

    public static AsterixFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return (AsterixFunctionInfo) finfoRepo.get(fid);
    }

    static {

        // first, take care of Algebricks builtin functions
        add(EQ, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(LE, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(GE, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(LT, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(GT, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(IS_NULL, ABooleanTypeComputer.INSTANCE);
        add(AND, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(NEQ, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(NOT, UnaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(OR, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(NUMERIC_ADD, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);

        // and then, Asterix builtin functions
        add(NOT_NULL, NotNullTypeComputer.INSTANCE);
        add(ANY_COLLECTION_MEMBER, NonTaggedCollectionMemberResultType.INSTANCE);
        addPrivateFunction(AVG, OptionalADoubleTypeComputer.INSTANCE);
        add(BOOLEAN_CONSTRUCTOR, UnaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(CARET, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(CIRCLE_CONSTRUCTOR, OptionalACircleTypeComputer.INSTANCE);
        add(CLOSED_RECORD_CONSTRUCTOR, ClosedRecordConstructorResultType.INSTANCE);
        add(CONCAT_NON_NULL, new IResultTypeComputer() {
            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
                if (f.getArguments().size() < 1) {
                    return BuiltinType.ANULL;
                }
                ILogicalExpression a0 = f.getArguments().get(0).getValue();
                IAType t0 = (IAType) env.getType(a0);
                if (TypeHelper.canBeNull(t0)) {
                    return t0;
                }
                return AUnionType.createNullableType(t0);
            }
        });
        add(CONTAINS, ABooleanTypeComputer.INSTANCE);
        addPrivateFunction(COUNT, AInt32TypeComputer.INSTANCE);
        add(COUNTHASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        add(COUNTHASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        add(CREATE_CIRCLE, ACircleTypeComputer.INSTANCE);
        add(CREATE_LINE, ALineTypeComputer.INSTANCE);
        add(CREATE_MBR, ADoubleTypeComputer.INSTANCE);
        add(CREATE_POINT, APointTypeComputer.INSTANCE);
        add(CREATE_POLYGON, APolygonTypeComputer.INSTANCE);
        add(CREATE_RECTANGLE, ARectangleTypeComputer.INSTANCE);

        add(DATE_CONSTRUCTOR, OptionalADateTypeComputer.INSTANCE);
        add(DATETIME_CONSTRUCTOR, OptionalADateTimeTypeComputer.INSTANCE);
        add(DOUBLE_CONSTRUCTOR, OptionalADoubleTypeComputer.INSTANCE);
        add(DURATION_CONSTRUCTOR, OptionalADurationTypeComputer.INSTANCE);
        add(EDIT_DISTANCE, AInt32TypeComputer.INSTANCE);
        add(EDIT_DISTANCE_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        add(EDIT_DISTANCE_STRING_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE);
        add(EDIT_DISTANCE_LIST_IS_FILTERABLE, ABooleanTypeComputer.INSTANCE);
        add(EMBED_TYPE, new IResultTypeComputer() {
            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return (IAType) BuiltinType.ANY;
            }
        });
        add(EMPTY_STREAM, ABooleanTypeComputer.INSTANCE);
        add(ENDS_WITH, ABooleanTypeComputer.INSTANCE);
        // add(FIELD_ACCESS, NonTaggedFieldAccessByNameResultType.INSTANCE);
        add(FIELD_ACCESS_BY_INDEX, FieldAccessByIndexResultType.INSTANCE);
        add(FIELD_ACCESS_BY_NAME, NonTaggedFieldAccessByNameResultType.INSTANCE);
        add(FLOAT_CONSTRUCTOR, OptionalAFloatTypeComputer.INSTANCE);
        add(FUZZY_EQ, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(GET_HANDLE, null); // TODO
        add(GET_ITEM, NonTaggedGetItemResultType.INSTANCE);
        add(GET_DATA, null); // TODO
        addPrivateFunction(GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        add(GRAM_TOKENS, OrderedListOfAStringTypeComputer.INSTANCE);
        add(GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        add(HASHED_GRAM_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        add(HASHED_WORD_TOKENS, OrderedListOfAInt32TypeComputer.INSTANCE);
        add(INDEX_SEARCH, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return BuiltinType.ANY; // TODO
            }
        });
        add(INT8_CONSTRUCTOR, OptionalAInt8TypeComputer.INSTANCE);
        add(INT16_CONSTRUCTOR, OptionalAInt16TypeComputer.INSTANCE);
        add(INT32_CONSTRUCTOR, OptionalAInt32TypeComputer.INSTANCE);
        add(INT64_CONSTRUCTOR, OptionalAInt64TypeComputer.INSTANCE);
        add(LEN, OptionalAInt32TypeComputer.INSTANCE);
        add(LIKE, BinaryBooleanOrNullFunctionTypeComputer.INSTANCE);
        add(LINE_CONSTRUCTOR, OptionalALineTypeComputer.INSTANCE);
        add(LISTIFY, OrderedListConstructorResultType.INSTANCE);
        addPrivateFunction(LOCAL_AVG, NonTaggedLocalAvgTypeComputer.INSTANCE);
        add(MAKE_FIELD_INDEX_HANDLE, null); // TODO
        add(MAKE_FIELD_NAME_HANDLE, null); // TODO
        add(MAX, NonTaggedSumTypeComputer.INSTANCE);
        add(LOCAL_MAX, NonTaggedSumTypeComputer.INSTANCE);
        add(MIN, NonTaggedSumTypeComputer.INSTANCE);
        add(LOCAL_MIN, NonTaggedSumTypeComputer.INSTANCE);
        add(NON_EMPTY_STREAM, ABooleanTypeComputer.INSTANCE);
        add(NULL_CONSTRUCTOR, ANullTypeComputer.INSTANCE);
        add(NUMERIC_UNARY_MINUS, NonTaggedUnaryMinusTypeComputer.INSTANCE);
        add(NUMERIC_SUBTRACT, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_MULTIPLY, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_DIVIDE, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_MOD, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_IDIV, AInt32TypeComputer.INSTANCE);

        add(NUMERIC_ABS, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        add(NUMERIC_CEILING, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        add(NUMERIC_FLOOR, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        add(NUMERIC_ROUND, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        add(NUMERIC_ROUND_HALF_TO_EVEN, NonTaggedNumericUnaryFunctionTypeComputer.INSTANCE);
        add(NUMERIC_ROUND_HALF_TO_EVEN2, NonTaggedNumericRoundHalfToEven2TypeComputer.INSTANCE);

        add(STRING_TO_CODEPOINT, OrderedListOfAInt32TypeComputer.INSTANCE);
        add(CODEPOINT_TO_STRING, AStringTypeComputer.INSTANCE);
        add(STRING_CONCAT, OptionalAStringTypeComputer.INSTANCE);
        add(SUBSTRING2, Substring2TypeComputer.INSTANCE);
        add(STRING_LENGTH, UnaryStringInt32OrNullTypeComputer.INSTANCE);
        add(STRING_LOWERCASE, UnaryStringOrNullTypeComputer.INSTANCE);
        add(STRING_START_WITH, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        add(STRING_END_WITH, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        add(STRING_MATCHES, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        add(STRING_MATCHES_WITH_FLAG, TripleStringBoolOrNullTypeComputer.INSTANCE);
        add(STRING_REPLACE, TripleStringStringOrNullTypeComputer.INSTANCE);
        add(STRING_REPLACE_WITH_FLAG, QuadStringStringOrNullTypeComputer.INSTANCE);
        add(SUBSTRING_BEFORE, BinaryStringStringOrNullTypeComputer.INSTANCE);
        add(SUBSTRING_AFTER, BinaryStringStringOrNullTypeComputer.INSTANCE);
        add(STRING_EQUAL, BinaryStringBoolOrNullTypeComputer.INSTANCE);
        add(STRING_JOIN, AStringTypeComputer.INSTANCE);

        add(OPEN_RECORD_CONSTRUCTOR, OpenRecordConstructorResultType.INSTANCE);
        add(ORDERED_LIST_CONSTRUCTOR, OrderedListConstructorResultType.INSTANCE);
        add(POINT_CONSTRUCTOR, OptionalAPointTypeComputer.INSTANCE);
        add(POINT3D_CONSTRUCTOR, OptionalAPoint3DTypeComputer.INSTANCE);
        add(POLYGON_CONSTRUCTOR, OptionalAPolygonTypeComputer.INSTANCE);
        add(PREFIX_LEN_JACCARD, AInt32TypeComputer.INSTANCE);
        add(RANGE, AInt32TypeComputer.INSTANCE);
        add(RECTANGLE_CONSTRUCTOR, OptionalARectangleTypeComputer.INSTANCE);
        // add(RECORD_TYPE_CONSTRUCTOR, null);
        add(SCALAR_AVG, ScalarVersionOfAggregateResultType.INSTANCE);
        add(SCALAR_COUNT, AInt32TypeComputer.INSTANCE);
        add(SCALAR_GLOBAL_AVG, ScalarVersionOfAggregateResultType.INSTANCE);
        add(SCALAR_LOCAL_AVG, ScalarVersionOfAggregateResultType.INSTANCE);
        add(SCALAR_MAX, ScalarVersionOfAggregateResultType.INSTANCE);
        add(SCALAR_MIN, ScalarVersionOfAggregateResultType.INSTANCE);
        add(SCALAR_SUM, ScalarVersionOfAggregateResultType.INSTANCE);
        add(SCAN_COLLECTION, NonTaggedCollectionMemberResultType.INSTANCE);
        add(SERIAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        add(SERIAL_COUNT, AInt32TypeComputer.INSTANCE);
        add(SERIAL_GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
        add(SERIAL_LOCAL_AVG, NonTaggedLocalAvgTypeComputer.INSTANCE);
        add(SERIAL_SUM, NonTaggedSumTypeComputer.INSTANCE);
        add(SERIAL_LOCAL_SUM, NonTaggedSumTypeComputer.INSTANCE);
        add(SIMILARITY_JACCARD, AFloatTypeComputer.INSTANCE);
        add(SIMILARITY_JACCARD_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        add(SIMILARITY_JACCARD_SORTED, AFloatTypeComputer.INSTANCE);
        add(SIMILARITY_JACCARD_SORTED_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        add(SIMILARITY_JACCARD_PREFIX, AFloatTypeComputer.INSTANCE);
        add(SIMILARITY_JACCARD_PREFIX_CHECK, OrderedListOfAnyTypeComputer.INSTANCE);
        add(SPATIAL_AREA, ADoubleTypeComputer.INSTANCE);
        add(SPATIAL_CELL, ARectangleTypeComputer.INSTANCE);
        add(SPATIAL_DISTANCE, ADoubleTypeComputer.INSTANCE);
        add(SPATIAL_INTERSECT, ABooleanTypeComputer.INSTANCE);
        add(GET_POINT_X_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE);
        add(GET_POINT_Y_COORDINATE_ACCESSOR, ADoubleTypeComputer.INSTANCE);
        add(GET_CIRCLE_RADIUS_ACCESSOR, ADoubleTypeComputer.INSTANCE);
        add(GET_CIRCLE_CENTER_ACCESSOR, APointTypeComputer.INSTANCE);
        add(GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR, OrderedListOfAPointTypeComputer.INSTANCE);
        add(STARTS_WITH, ABooleanTypeComputer.INSTANCE);
        add(STRING_CONSTRUCTOR, OptionalAStringTypeComputer.INSTANCE);
        add(SUBSET_COLLECTION, new IResultTypeComputer() {

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
        add(SUBSTRING, SubstringTypeComputer.INSTANCE);
        addPrivateFunction(SUM, NonTaggedSumTypeComputer.INSTANCE);
        add(LOCAL_SUM, NonTaggedSumTypeComputer.INSTANCE);
        add(SWITCH_CASE, NonTaggedSwitchCaseComputer.INSTANCE);
        add(REG_EXP, ABooleanTypeComputer.INSTANCE);
        add(INJECT_FAILURE, InjectFailureTypeComputer.INSTANCE);
        add(CAST_RECORD, CastRecordResultTypeComputer.INSTANCE);
        add(CAST_LIST, CastListResultTypeComputer.INSTANCE);

        add(TID, AInt32TypeComputer.INSTANCE);
        add(TIME_CONSTRUCTOR, OptionalATimeTypeComputer.INSTANCE);
        add(TYPE_OF, null); // TODO
        add(UNORDERED_LIST_CONSTRUCTOR, UnorderedListConstructorResultType.INSTANCE);
        add(WORD_TOKENS, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return new AOrderedListType(BuiltinType.ASTRING, "string");
            }
        });

        // temporal type accessors
        add(ACCESSOR_TEMPORAL_YEAR, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_MONTH, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_DAY, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_HOUR, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_MIN, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_SEC, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_MILLISEC, OptionalAInt32TypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_INTERVAL_START, OptionalATemporalInstanceTypeComputer.INSTANCE);
        add(ACCESSOR_TEMPORAL_INTERVAL_END, OptionalATemporalInstanceTypeComputer.INSTANCE);

        // temporal functions
        add(DATE_FROM_UNIX_TIME_IN_DAYS, OptionalADateTypeComputer.INSTANCE);
        add(DATE_FROM_DATETIME, OptionalADateTypeComputer.INSTANCE);
        add(ADD_DATE_DURATION, OptionalADateTypeComputer.INSTANCE);
        add(SUBTRACT_DATE, OptionalADurationTypeComputer.INSTANCE);
        add(TIME_FROM_UNIX_TIME_IN_MS, OptionalATimeTypeComputer.INSTANCE);
        add(TIME_FROM_DATETIME, OptionalATimeTypeComputer.INSTANCE);
        add(SUBTRACT_TIME, OptionalADurationTypeComputer.INSTANCE);
        add(ADD_TIME_DURATION, OptionalATimeTypeComputer.INSTANCE);
        add(DATETIME_FROM_DATE_TIME, OptionalADateTimeTypeComputer.INSTANCE);
        add(DATETIME_FROM_UNIX_TIME_IN_MS, OptionalADateTimeTypeComputer.INSTANCE);
        add(SUBTRACT_DATETIME, OptionalADurationTypeComputer.INSTANCE);
        add(ADD_DATETIME_DURATION, OptionalADateTimeTypeComputer.INSTANCE);
        add(CALENDAR_DURATION_FROM_DATETIME, OptionalADurationTypeComputer.INSTANCE);
        add(CALENDAR_DURATION_FROM_DATE, OptionalADurationTypeComputer.INSTANCE);
        add(ADJUST_DATETIME_FOR_TIMEZONE, OptionalAStringTypeComputer.INSTANCE);
        add(ADJUST_TIME_FOR_TIMEZONE, OptionalAStringTypeComputer.INSTANCE);
        add(INTERVAL_BEFORE, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_AFTER, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_MEETS, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_MET_BY, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_OVERLAPS, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_OVERLAPPED_BY, OptionalABooleanTypeComputer.INSTANCE);
        add(OVERLAP, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_STARTS, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_STARTED_BY, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_COVERS, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_COVERED_BY, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_ENDS, OptionalABooleanTypeComputer.INSTANCE);
        add(INTERVAL_ENDED_BY, OptionalABooleanTypeComputer.INSTANCE);
        add(CURRENT_DATE, ADateTypeComputer.INSTANCE);
        add(CURRENT_TIME, ATimeTypeComputer.INSTANCE);
        add(CURRENT_DATETIME, ADateTimeTypeComputer.INSTANCE);
        add(DAY_TIME_DURATION_GREATER_THAN, OptionalABooleanTypeComputer.INSTANCE);
        add(DAY_TIME_DURATION_LESS_THAN, OptionalABooleanTypeComputer.INSTANCE);
        add(YEAR_MONTH_DURATION_GREATER_THAN, OptionalABooleanTypeComputer.INSTANCE);
        add(YEAR_MONTH_DURATION_LESS_THAN, OptionalABooleanTypeComputer.INSTANCE);
        add(DURATION_EQUAL, OptionalABooleanTypeComputer.INSTANCE);
        add(DURATION_FROM_MONTHS, OptionalADurationTypeComputer.INSTANCE);
        add(DURATION_FROM_MILLISECONDS, OptionalADurationTypeComputer.INSTANCE);
        add(MONTHS_OF_YEAR_MONTH_DURATION, OptionalAInt32TypeComputer.INSTANCE);
        add(MILLISECONDS_OF_DAY_TIME_DURATION, OptionalAInt64TypeComputer.INSTANCE);

        // interval constructors
        add(INTERVAL_CONSTRUCTOR_DATE, OptionalAIntervalTypeComputer.INSTANCE);
        add(INTERVAL_CONSTRUCTOR_TIME, OptionalAIntervalTypeComputer.INSTANCE);
        add(INTERVAL_CONSTRUCTOR_DATETIME, OptionalAIntervalTypeComputer.INSTANCE);
        add(INTERVAL_CONSTRUCTOR_START_FROM_DATE, OptionalAIntervalTypeComputer.INSTANCE);
        add(INTERVAL_CONSTRUCTOR_START_FROM_DATETIME, OptionalAIntervalTypeComputer.INSTANCE);
        add(INTERVAL_CONSTRUCTOR_START_FROM_TIME, OptionalAIntervalTypeComputer.INSTANCE);

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

    public static boolean isBuiltinCompilerFunction(FunctionIdentifier fi) {
        return builtinFunctionsSet.keySet().contains(getAsterixFunctionInfo(fi));
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

    public static void add(FunctionIdentifier fi, IResultTypeComputer typeComputer) {
        IFunctionInfo functionInfo = getAsterixFunctionInfo(fi);
        builtinFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        finfoRepo.put(fi);
    }

    private static IFunctionInfo addPrivateFunction(FunctionIdentifier fi, IResultTypeComputer typeComputer) {
        IFunctionInfo functionInfo = getAsterixFunctionInfo(fi);
        builtinFunctionsSet.put(functionInfo, functionInfo);
        funTypeComputer.put(functionInfo, typeComputer);
        return functionInfo;
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

}