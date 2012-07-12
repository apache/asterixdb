package edu.uci.ics.asterix.om.functions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ABooleanTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ACircleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ADoubleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AFloatTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AInt32TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ALineTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ANullTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.APointTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.APolygonTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ARectangleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.AStringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.BinaryBooleanOrNullFunctionTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.CastRecordResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ClosedRecordConstructorResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.FieldAccessByIndexResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.InjectFailureTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedCollectionMemberResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedFieldAccessByNameResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedGetItemResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedLocalAvgTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedNumericAddSubMulDivTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedSumTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedSwitchCaseComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.NonTaggedUnaryMinusTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OpenRecordConstructorResultType;
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
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalALineTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAPoint3DTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAPointTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAPolygonTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalARectangleTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalAStringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OptionalATimeTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListConstructorResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAInt32TypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAStringTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.OrderedListOfAnyTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.impl.ScalarVersionOfAggregateResultType;
import edu.uci.ics.asterix.om.typecomputer.impl.UnaryBooleanOrNullFunctionTypeComputer;
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

    private final static Map<FunctionIdentifier, IFunctionInfo> asterixFunctionIdToInfo = new HashMap<FunctionIdentifier, IFunctionInfo>();

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
    public final static FunctionIdentifier RECORD_TYPE_CONSTRUCTOR = new FunctionIdentifier(
            FunctionConstants.ASTERIX_NS, "record-type-constructor", FunctionIdentifier.VARARGS);
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
    private final static FunctionIdentifier STARTS_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "starts-with", 2);
    private final static FunctionIdentifier ENDS_WITH = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ends-with", 2);

    public final static FunctionIdentifier AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-avg", 1);
    public final static FunctionIdentifier COUNT = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-count", 1);
    public final static FunctionIdentifier SUM = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-sum", 1);
    public final static FunctionIdentifier MAX = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-max", 1);
    public final static FunctionIdentifier MIN = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "agg-min", 1);
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
    public final static FunctionIdentifier SERIAL_GLOBAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "global-avg-serial", 1);
    public final static FunctionIdentifier SERIAL_LOCAL_AVG = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "local-avg-serial", 1);

    public final static FunctionIdentifier YEAR = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "year", 1);

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

    public static IFunctionInfo getAsterixFunctionInfo(FunctionIdentifier fid) {
        IFunctionInfo finfo = asterixFunctionIdToInfo.get(fid);
        if (finfo == null) {
            finfo = new AsterixFunctionInfo(fid);
            //   if (fid.isBuiltin()) {
            asterixFunctionIdToInfo.put(fid, finfo);
            //  }
        }
        return finfo;
    }

    public static AsterixFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return (AsterixFunctionInfo) asterixFunctionIdToInfo.get(fid);
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
        add(ANY_COLLECTION_MEMBER, NonTaggedCollectionMemberResultType.INSTANCE);
        add(AVG, OptionalADoubleTypeComputer.INSTANCE);
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
        add(COUNT, AInt32TypeComputer.INSTANCE);
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
        add(GLOBAL_AVG, OptionalADoubleTypeComputer.INSTANCE);
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
        add(LOCAL_AVG, NonTaggedLocalAvgTypeComputer.INSTANCE);
        add(MAKE_FIELD_INDEX_HANDLE, null); // TODO
        add(MAKE_FIELD_NAME_HANDLE, null); // TODO
        add(MAX, NonTaggedSumTypeComputer.INSTANCE);
        add(MIN, NonTaggedSumTypeComputer.INSTANCE);
        add(NON_EMPTY_STREAM, ABooleanTypeComputer.INSTANCE);
        add(NULL_CONSTRUCTOR, ANullTypeComputer.INSTANCE);
        add(NUMERIC_UNARY_MINUS, NonTaggedUnaryMinusTypeComputer.INSTANCE);
        add(NUMERIC_SUBTRACT, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_MULTIPLY, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_DIVIDE, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_MOD, NonTaggedNumericAddSubMulDivTypeComputer.INSTANCE);
        add(NUMERIC_IDIV, AInt32TypeComputer.INSTANCE);
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
        add(SCALAR_COUNT, ScalarVersionOfAggregateResultType.INSTANCE);
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
        add(SUBSTRING, AStringTypeComputer.INSTANCE);
        add(SUM, NonTaggedSumTypeComputer.INSTANCE);
        add(SWITCH_CASE, NonTaggedSwitchCaseComputer.INSTANCE);
        add(REG_EXP, ABooleanTypeComputer.INSTANCE);
        add(INJECT_FAILURE, InjectFailureTypeComputer.INSTANCE);
        add(CAST_RECORD, CastRecordResultTypeComputer.INSTANCE);

        add(TID, AInt32TypeComputer.INSTANCE);
        add(TIME_CONSTRUCTOR, OptionalATimeTypeComputer.INSTANCE);
        add(TYPE_OF, null); // TODO
        add(UNORDERED_LIST_CONSTRUCTOR, UnorderedListConstructorResultType.INSTANCE);
        add(YEAR, OptionalAInt32TypeComputer.INSTANCE);
        add(WORD_TOKENS, new IResultTypeComputer() {

            @Override
            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                    IMetadataProvider<?, ?> mp) throws AlgebricksException {
                return new AOrderedListType(BuiltinType.ASTRING, "string");
            }
        });

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
        addLocalAgg(MAX, MAX);
        addGlobalAgg(MAX, MAX);

        addAgg(MIN);
        addLocalAgg(MIN, MIN);
        addGlobalAgg(MIN, MIN);

        addAgg(SUM);
        addLocalAgg(SUM, SUM);
        addGlobalAgg(SUM, SUM);

        addAgg(LISTIFY);

        // serializable aggregate functions
        addSerialAgg(AVG, SERIAL_AVG);
        addSerialAgg(COUNT, SERIAL_COUNT);
        addSerialAgg(SUM, SERIAL_SUM);
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
        addLocalAgg(SERIAL_SUM, SERIAL_SUM);
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
        asterixFunctionIdToInfo.put(fi, functionInfo);
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