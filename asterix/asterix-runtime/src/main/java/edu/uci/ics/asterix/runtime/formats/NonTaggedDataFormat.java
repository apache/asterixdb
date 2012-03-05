package edu.uci.ics.asterix.runtime.formats;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.dataflow.data.nontagged.AqlNullWriterFactory;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryBooleanInspectorImpl;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryIntegerInspector;
import edu.uci.ics.asterix.formats.nontagged.AqlNormalizedKeyComputerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.FunctionManagerHolder;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionManager;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableCountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableGlobalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableLocalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.serializable.std.SerializableSumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.AvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.CountAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.GlobalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.LocalAvgAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.MaxAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.MinAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.std.SumAggregateDescriptor;
import edu.uci.ics.asterix.runtime.aggregates.stream.NonEmptyStreamAggregateDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.CreateMBREvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.common.FieldAccessByIndexEvalFactory;
import edu.uci.ics.asterix.runtime.evaluators.common.FunctionManagerImpl;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ABooleanConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ACircleConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADateConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADateTimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADoubleConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ADurationConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AFloatConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt16ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt32ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt64ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AInt8ConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ALineConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ANullConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.APoint3DConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.APointConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.APolygonConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ARectangleConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.AStringConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.constructors.ATimeConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.AndDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.AnyCollectionMemberDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.ClosedRecordConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.ContainsDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CountHashedGramTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CountHashedWordTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateCircleDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateLineDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateMBRDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreatePointDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreatePolygonDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.CreateRectangleDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EditDistanceDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EmbedTypeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.EndsWithDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.FieldAccessByIndexDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.FieldAccessByNameDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.FuzzyEqDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.GetItemDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.GramTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.HashedGramTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.HashedWordTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.InjectFailureDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.IsNullDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.LenDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.LikeDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NotDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericAddDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericDivideDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericMultiplyDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericSubtractDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.NumericUnaryMinusDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.OpenRecordConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.OrDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.OrderedListConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.PrefixLenJaccardDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.RegExpDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardPrefixCheckDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SimilarityJaccardPrefixDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialAreaDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialCellDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialDistanceDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SpatialIntersectDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.StartsWithDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SubstringDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.SwitchCaseDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.UnorderedListConstructorDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.WordTokensDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.functions.YearDescriptor;
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.RangeDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.std.SubsetCollectionDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.evaluators.ColumnAccessEvalFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.evaluators.ConstantEvalFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class NonTaggedDataFormat implements IDataFormat {

    private static boolean registered = false;

    public static final NonTaggedDataFormat INSTANCE = new NonTaggedDataFormat();

    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    private static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<ATypeTag, IValueParserFactory>();
    static {
        typeToValueParserFactMap.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
    }

    public NonTaggedDataFormat() {
    }

    public void registerRuntimeFunctions() throws AlgebricksException {

        if (registered) {
            return;
        }
        registered = true;

        if (FunctionManagerHolder.getFunctionManager() != null) {
            return;
        }

        List<IFunctionDescriptor> temp = new ArrayList<IFunctionDescriptor>();

        // format-independent
        temp.add(new ContainsDescriptor());
        temp.add(new EndsWithDescriptor());
        temp.add(new StartsWithDescriptor());
        temp.add(new SubstringDescriptor());
        temp.add(new TidRunningAggregateDescriptor());

        // format-dependent
        temp.add(new AndDescriptor());
        temp.add(new OrDescriptor());
        temp.add(new LikeDescriptor());
        temp.add(new YearDescriptor());
        temp.add(new ScanCollectionDescriptor());
        temp.add(new AnyCollectionMemberDescriptor());
        temp.add(new ClosedRecordConstructorDescriptor());
        temp.add(new FieldAccessByIndexDescriptor());
        temp.add(new FieldAccessByNameDescriptor());
        temp.add(new GetItemDescriptor());
        temp.add(new NumericUnaryMinusDescriptor());
        temp.add(new OpenRecordConstructorDescriptor());
        temp.add(new OrderedListConstructorDescriptor());
        temp.add(new UnorderedListConstructorDescriptor());
        temp.add(new EmbedTypeDescriptor());

        temp.add(new NumericAddDescriptor());
        temp.add(new NumericDivideDescriptor());
        temp.add(new NumericMultiplyDescriptor());
        temp.add(new NumericSubtractDescriptor());
        temp.add(new IsNullDescriptor());
        temp.add(new NotDescriptor());
        temp.add(new LenDescriptor());
        temp.add(new NonEmptyStreamAggregateDescriptor());
        temp.add(new RangeDescriptor());

        // aggregates
        temp.add(new ListifyAggregateDescriptor());
        temp.add(new CountAggregateDescriptor());
        temp.add(new AvgAggregateDescriptor());
        temp.add(new LocalAvgAggregateDescriptor());
        temp.add(new GlobalAvgAggregateDescriptor());
        temp.add(new SumAggregateDescriptor());
        temp.add(new MaxAggregateDescriptor());
        temp.add(new MinAggregateDescriptor());

        // serializable aggregates
        temp.add(new SerializableCountAggregateDescriptor());
        temp.add(new SerializableAvgAggregateDescriptor());
        temp.add(new SerializableLocalAvgAggregateDescriptor());
        temp.add(new SerializableGlobalAvgAggregateDescriptor());
        temp.add(new SerializableSumAggregateDescriptor());

        // new functions - constructors
        temp.add(new ABooleanConstructorDescriptor());
        temp.add(new ANullConstructorDescriptor());
        temp.add(new AStringConstructorDescriptor());
        temp.add(new AInt8ConstructorDescriptor());
        temp.add(new AInt16ConstructorDescriptor());
        temp.add(new AInt32ConstructorDescriptor());
        temp.add(new AInt64ConstructorDescriptor());
        temp.add(new AFloatConstructorDescriptor());
        temp.add(new ADoubleConstructorDescriptor());
        temp.add(new APointConstructorDescriptor());
        temp.add(new APoint3DConstructorDescriptor());
        temp.add(new ALineConstructorDescriptor());
        temp.add(new APolygonConstructorDescriptor());
        temp.add(new ACircleConstructorDescriptor());
        temp.add(new ARectangleConstructorDescriptor());
        temp.add(new ATimeConstructorDescriptor());
        temp.add(new ADateConstructorDescriptor());
        temp.add(new ADateTimeConstructorDescriptor());
        temp.add(new ADurationConstructorDescriptor());

        // Spatial
        temp.add(new CreatePointDescriptor());
        temp.add(new CreateLineDescriptor());
        temp.add(new CreatePolygonDescriptor());
        temp.add(new CreateCircleDescriptor());
        temp.add(new CreateRectangleDescriptor());
        temp.add(new SpatialAreaDescriptor());
        temp.add(new SpatialDistanceDescriptor());
        temp.add(new SpatialIntersectDescriptor());
        temp.add(new CreateMBRDescriptor());
        temp.add(new SpatialCellDescriptor());

        // fuzzyjoin function
        temp.add(new FuzzyEqDescriptor());
        temp.add(new SubsetCollectionDescriptor());
        temp.add(new PrefixLenJaccardDescriptor());

        temp.add(new WordTokensDescriptor());
        temp.add(new HashedWordTokensDescriptor());
        temp.add(new CountHashedWordTokensDescriptor());

        temp.add(new GramTokensDescriptor());
        temp.add(new HashedGramTokensDescriptor());
        temp.add(new CountHashedGramTokensDescriptor());

        temp.add(new EditDistanceDescriptor());
        temp.add(new EditDistanceCheckDescriptor());

        temp.add(new SimilarityJaccardDescriptor());
        temp.add(new SimilarityJaccardCheckDescriptor());
        temp.add(new SimilarityJaccardPrefixDescriptor());
        temp.add(new SimilarityJaccardPrefixCheckDescriptor());

        temp.add(new SwitchCaseDescriptor());
        temp.add(new RegExpDescriptor());
        temp.add(new InjectFailureDescriptor());

        IFunctionManager mgr = new FunctionManagerImpl();
        for (IFunctionDescriptor fd : temp) {
            mgr.registerFunction(fd);
        }
        FunctionManagerHolder.setFunctionManager(mgr);
    }

    @Override
    public IBinaryBooleanInspector getBinaryBooleanInspector() {
        return AqlBinaryBooleanInspectorImpl.INSTANCE;
    }

    @Override
    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return AqlBinaryComparatorFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return AqlBinaryHashFunctionFactoryProvider.INSTANCE;
    }

    @Override
    public ISerializerDeserializerProvider getSerdeProvider() {
        return AqlSerializerDeserializerProvider.INSTANCE; // done
    }

    @Override
    public ITypeTraitProvider getTypeTraitProvider() {
        return AqlTypeTraitProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEvaluatorFactory getFieldAccessEvaluatorFactory(ARecordType recType, String fldName, int recordColumn)
            throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        for (int i = 0; i < n; i++) {
            if (names[i].equals(fldName)) {
                IEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(recordColumn);
                ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                DataOutput dos = abvs.getDataOutput();
                try {
                    AInt32 ai = new AInt32(i);
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
                IEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs.getBytes(),
                        abvs.getLength()));
                IEvaluatorFactory evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory,
                        fldIndexEvalFactory, recType);
                return evalFactory;
            }
        }
        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEvaluatorFactory[] createMBRFactory(ARecordType recType, String fldName, int recordColumn, int dimension)
            throws AlgebricksException {
        IEvaluatorFactory evalFactory = getFieldAccessEvaluatorFactory(recType, fldName, recordColumn);
        int numOfFields = dimension * 2;
        IEvaluatorFactory[] evalFactories = new IEvaluatorFactory[numOfFields];

        ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
        DataOutput dos1 = abvs1.getDataOutput();
        try {
            AInt32 ai = new AInt32(dimension);
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos1);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        IEvaluatorFactory dimensionEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs1.getBytes(),
                abvs1.getLength()));

        for (int i = 0; i < numOfFields; i++) {
            ArrayBackedValueStorage abvs2 = new ArrayBackedValueStorage();
            DataOutput dos2 = abvs2.getDataOutput();
            try {
                AInt32 ai = new AInt32(i);
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos2);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            IEvaluatorFactory coordinateEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs2.getBytes(),
                    abvs2.getLength()));

            evalFactories[i] = new CreateMBREvalFactory(evalFactory, dimensionEvalFactory, coordinateEvalFactory);
        }
        return evalFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            ARecordType recType, String fldName) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        for (int i = 0; i < n; i++) {
            if (names[i].equals(fldName)) {
                IEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                        GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
                ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                DataOutput dos = abvs.getDataOutput();
                try {
                    AInt32 ai = new AInt32(i);
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
                IEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(Arrays.copyOf(abvs.getBytes(),
                        abvs.getLength()));
                IEvaluatorFactory evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory,
                        fldIndexEvalFactory, recType);
                IFunctionInfo finfoAccess = FunctionUtils
                        .getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX);
                ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                        new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                return new Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory, partitionFun,
                        recType.getFieldTypes()[i]);
            }
        }
        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @Override
    public IFunctionDescriptor resolveFunction(ILogicalExpression expr, IVariableTypeEnvironment context)
            throws AlgebricksException {
        FunctionIdentifier fnId = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier();
        IFunctionManager mgr = FunctionManagerHolder.getFunctionManager();
        IFunctionDescriptor fd = mgr.lookupFunction(fnId);
        if (fd == null) {
            throw new AsterixRuntimeException("Unresolved function " + fnId);
        }
        typeInference(expr, fd, context);
        return fd;
    }

    private void typeInference(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
            throws AlgebricksException {
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.LISTIFY)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            if (f.getArguments().size() == 0) {
                ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(null, null));
            } else {
                IAType itemType = (IAType) context.getType(f.getArguments().get(0).getValue());
                ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(itemType, null));
            }
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
            ARecordType rt = (ARecordType) context.getType(expr);
            ((OpenRecordConstructorDescriptor) fd).reset(rt,
                    computeOpenFields((AbstractFunctionCallExpression) expr, rt));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            ((ClosedRecordConstructorDescriptor) fd).reset((ARecordType) context.getType(expr));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)) {
            ((OrderedListConstructorDescriptor) fd).reset((AOrderedListType) context.getType(expr));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR)) {
            ((UnorderedListConstructorDescriptor) fd).reset((AUnorderedListType) context.getType(expr));
        }
        if (fd.getIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            switch (t.getTypeTag()) {
                case RECORD: {
                    ARecordType recType = (ARecordType) t;
                    ((FieldAccessByIndexDescriptor) fd).reset(recType);
                    break;
                }
                case UNION: {
                    AUnionType unionT = (AUnionType) t;
                    if (unionT.isNullableType()) {
                        IAType t2 = unionT.getUnionList().get(1);
                        if (t2.getTypeTag() == ATypeTag.RECORD) {
                            ARecordType recType = (ARecordType) t2;
                            ((FieldAccessByIndexDescriptor) fd).reset(recType);
                            break;
                        }
                    }
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
                default: {
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
            }
        }
    }

    private boolean[] computeOpenFields(AbstractFunctionCallExpression expr, ARecordType recType) {
        int n = expr.getArguments().size() / 2;
        boolean[] open = new boolean[n];
        for (int i = 0; i < n; i++) {
            Mutable<ILogicalExpression> argRef = expr.getArguments().get(2 * i);
            ILogicalExpression arg = argRef.getValue();
            if (arg.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                String fn = ((AString) ((AsterixConstantValue) ((ConstantExpression) arg).getValue()).getObject())
                        .getStringValue();
                open[i] = true;
                for (String s : recType.getFieldNames()) {
                    if (s.equals(fn)) {
                        open[i] = false;
                        break;
                    }
                }
            } else {
                open[i] = true;
            }
        }
        return open;
    }

    @Override
    public IPrinterFactoryProvider getPrinterFactoryProvider() {
        return AqlPrinterFactoryProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException {
        IAObject obj = null;
        if (value.isNull()) {
            obj = ANull.NULL;
        } else if (value.isTrue()) {
            obj = ABoolean.TRUE;
        } else if (value.isFalse()) {
            obj = ABoolean.FALSE;
        } else {
            AsterixConstantValue acv = (AsterixConstantValue) value;
            obj = acv.getObject();
        }
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        try {
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(obj.getType()).serialize(obj, dos);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return new ConstantEvalFactory(Arrays.copyOf(abvs.getBytes(), abvs.getLength()));
    }

    @Override
    public IBinaryIntegerInspector getBinaryIntegerInspector() {
        return AqlBinaryIntegerInspector.INSTANCE;
    }

    @Override
    public ITupleParserFactory createTupleParser(ARecordType recType, IParseFileSplitsDecl decl) {
        if (decl.isDelimitedFileFormat()) {
            int n = recType.getFieldTypes().length;
            IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
            for (int i = 0; i < n; i++) {
                ATypeTag tag = recType.getFieldTypes()[i].getTypeTag();
                IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
                if (vpf == null) {
                    throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
                }
                fieldParserFactories[i] = vpf;
            }
            return new NtDelimitedDataTupleParserFactory(recType, fieldParserFactories, decl.getDelimChar());
        } else {
            return new AdmSchemafullRecordParserFactory(recType);
        }
    }

    @Override
    public ITupleParserFactory createTupleParser(ARecordType recType, boolean delimitedFormat, Character delimiter) {
        if (delimitedFormat) {
            int n = recType.getFieldTypes().length;
            IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
            for (int i = 0; i < n; i++) {
                ATypeTag tag = recType.getFieldTypes()[i].getTypeTag();
                IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
                if (vpf == null) {
                    throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
                }
                fieldParserFactories[i] = vpf;
            }
            return new NtDelimitedDataTupleParserFactory(recType, fieldParserFactories, delimiter);
        } else {
            return new AdmSchemafullRecordParserFactory(recType);
        }
    }

    @Override
    public INullWriterFactory getNullWriterFactory() {
        return AqlNullWriterFactory.INSTANCE;
    }

    @Override
    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return new IExpressionEvalSizeComputer() {
            @Override
            public int getEvalSize(ILogicalExpression expr, IVariableEvalSizeEnvironment env)
                    throws AlgebricksException {
                switch (expr.getExpressionTag()) {
                    case CONSTANT: {
                        ConstantExpression c = (ConstantExpression) expr;
                        if (c == ConstantExpression.NULL) {
                            return 1;
                        } else if (c == ConstantExpression.FALSE || c == ConstantExpression.TRUE) {
                            return 2;
                        } else {
                            AsterixConstantValue acv = (AsterixConstantValue) c.getValue();
                            IAObject o = acv.getObject();
                            switch (o.getType().getTypeTag()) {
                                case DOUBLE: {
                                    return 9;
                                }
                                case BOOLEAN: {
                                    return 2;
                                }
                                case NULL: {
                                    return 1;
                                }
                                case INT32: {
                                    return 5;
                                }
                                case INT64: {
                                    return 9;
                                }
                                default: {
                                    // TODO
                                    return -1;
                                }
                            }
                        }
                    }
                    case FUNCTION_CALL: {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.TID)) {
                            return 5;
                        } else {
                            // TODO
                            return -1;
                        }
                    }
                    default: {
                        // TODO
                        return -1;
                    }
                }
            }
        };
    }

    @Override
    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return AqlNormalizedKeyComputerFactoryProvider.INSTANCE;
    }

}
