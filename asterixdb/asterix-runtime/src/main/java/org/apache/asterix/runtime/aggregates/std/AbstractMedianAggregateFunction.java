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

package org.apache.asterix.runtime.aggregates.std;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.NormalizedKeyComputerFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.typecomputer.impl.LocalMedianTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.comm.channels.FileNetworkInputChannel;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.net.NetworkManager;
import org.apache.hyracks.control.nc.partitions.JobFileState;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.collectors.InputChannelFrameReader;
import org.apache.hyracks.dataflow.std.sort.RunMergingFrameReader;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractMedianAggregateFunction extends AbstractAggregateFunction {

    private static final Logger LOGGER = LogManager.getLogger();

    protected static final String MEDIAN = "median";
    private static final int COUNT_FIELD_ID = 0;
    private static final int HANDLE_FIELD_ID = 1;
    private static final int ADDRESS_FIELD_ID = 2;
    private static final int PORT_FIELD_ID = 3;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> longSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt32> intSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    protected final IBinaryComparatorFactory doubleComparatorFactory =
            BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(ATypeTag.DOUBLE, true);
    protected final INormalizedKeyComputerFactory doubleNkComputerFactory =
            NormalizedKeyComputerFactoryProvider.INSTANCE.getNormalizedKeyComputerFactory(BuiltinType.ADOUBLE, true);
    protected final RecordDescriptor recordDesc = new RecordDescriptor(new ISerializerDeserializer[] { doubleSerde },
            new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ADOUBLE) });

    protected final AMutableString aString = new AMutableString("");
    protected final AMutableInt64 aInt64 = new AMutableInt64(0);
    protected final AMutableInt32 aInt32 = new AMutableInt32(0);
    protected final AMutableDouble aDouble = new AMutableDouble(0);
    protected final IPointable inputVal = new VoidPointable();
    private final FrameTupleReference ftr = new FrameTupleReference();
    private final FrameTupleAccessor fta = new FrameTupleAccessor(recordDesc);
    protected final List<IFrameReader> readers = new ArrayList<>();

    protected final IScalarEvaluator eval;
    protected final IEvaluatorContext ctx;
    protected final IFrame frame;
    protected long count;
    private List<IFrame> inFrames;
    private List<PartialResult> partialResults;
    private RecordBuilder recBuilder;

    public AbstractMedianAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        ctx = context;
        eval = args[0].createScalarEvaluator(context);
        frame = new VSizeFrame(context.getTaskContext());
    }

    @Override
    public void init() throws HyracksDataException {
        if (partialResults == null) {
            partialResults = new ArrayList<>();
        }
        if (recBuilder == null) {
            recBuilder = new RecordBuilder();
            recBuilder.reset(LocalMedianTypeComputer.REC_TYPE);
        }
        count = 0;
        partialResults.clear();
        recBuilder.init();
    }

    protected void processPartialResults(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        byte[] serBytes = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[offset]);
        if (typeTag == ATypeTag.OBJECT) {
            long handleCount = AInt64SerializerDeserializer.getLong(serBytes,
                    ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, COUNT_FIELD_ID, 0, false));
            if (handleCount == 0) {
                return;
            }
            count += handleCount;

            long fileId = AInt64SerializerDeserializer.getLong(serBytes,
                    ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, HANDLE_FIELD_ID, 0, false));

            String address = UTF8StringUtil.toString(serBytes,
                    ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, ADDRESS_FIELD_ID, 0, false));

            int port = AInt32SerializerDeserializer.getInt(serBytes,
                    ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, PORT_FIELD_ID, 0, false));

            partialResults.add(new PartialResult(fileId, handleCount, address, port));
        } else {
            throw new UnsupportedItemTypeException(sourceLoc, MEDIAN, serBytes[offset]);
        }
    }

    protected void finishPartialResult(IPointable result) throws HyracksDataException {
        if (count == 0) {
            setPartialResult(result, -1, "", -1);
            return;
        }

        IHyracksTaskContext taskCtx = ctx.getTaskContext();
        IHyracksJobletContext jobletCtx = taskCtx.getJobletContext();
        RunMergingFrameReader merger = createRunsMergingFrameReader();
        FileReference managedFile = jobletCtx.createManagedWorkspaceFile(MEDIAN);
        RunFileWriter runFileWriter = new RunFileWriter(managedFile, taskCtx.getIoManager());
        merger.open();
        runFileWriter.open();
        try {
            while (merger.nextFrame(frame)) {
                runFileWriter.nextFrame(frame.getBuffer());
            }
        } finally {
            runFileWriter.close();
            merger.close();
        }

        NetworkAddress netAddress = ((NodeControllerService) jobletCtx.getServiceContext().getControllerService())
                .getNetworkManager().getPublicNetworkAddress();

        long fileId = jobletCtx.nextUniqueId();
        taskCtx.setStateObject(new JobFileState(managedFile, jobletCtx.getJobId(), fileId));
        setPartialResult(result, fileId, netAddress.getAddress(), netAddress.getPort());
    }

    protected void finishFinalResult(IPointable result) throws HyracksDataException {
        if (count == 0) {
            PointableHelper.setNull(result);
            return;
        }
        try {
            boolean medianFound = findMedian();
            resultStorage.reset();
            if (medianFound) {
                doubleSerde.serialize(aDouble, resultStorage.getDataOutput());
                result.set(resultStorage);
            } else {
                PointableHelper.setNull(result);
                LOGGER.warn("median was not found");
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private boolean findMedian() throws HyracksDataException {
        RunMergingFrameReader merger = createRunsMergingFrameReader();
        return getMedian(merger);
    }

    protected RunMergingFrameReader createRunsMergingFrameReader() throws HyracksDataException {
        IHyracksTaskContext taskCtx = ctx.getTaskContext();
        IHyracksJobletContext jobletCtx = taskCtx.getJobletContext();
        INCServiceContext serviceCtx = jobletCtx.getServiceContext();
        NetworkManager netManager = ((NodeControllerService) serviceCtx.getControllerService()).getNetworkManager();
        List<IFrame> inFrames = getInFrames(partialResults.size(), taskCtx);
        readers.clear();
        for (PartialResult partialResult : partialResults) {
            IFrameReader inputChannelReader = createInputChannel(netManager, taskCtx,
                    new NetworkAddress(partialResult.address, partialResult.port), jobletCtx.getJobId().getId(),
                    partialResult.fileId);
            readers.add(inputChannelReader);
        }
        return new RunMergingFrameReader(taskCtx, readers, inFrames, new int[] { 0 },
                new IBinaryComparator[] { doubleComparatorFactory.createBinaryComparator() },
                doubleNkComputerFactory.createNormalizedKeyComputer(), recordDesc);
    }

    private boolean getMedian(RunMergingFrameReader merger) throws HyracksDataException {
        boolean isOdd = count % 2 != 0;
        long medianPosition = isOdd ? count / 2 : (count - 1) / 2;
        long currentTupleCount = 0;
        boolean found = false;
        merger.open();
        try {
            while (merger.nextFrame(frame)) {
                fta.reset(frame.getBuffer());
                int tupleCount = fta.getTupleCount();
                if (currentTupleCount + tupleCount > medianPosition) {
                    int firstMedian = (int) (medianPosition - currentTupleCount);
                    ftr.reset(fta, firstMedian);
                    double medianVal =
                            ADoubleSerializerDeserializer.getDouble(ftr.getFieldData(0), ftr.getFieldStart(0) + 1);
                    if (!isOdd) {
                        if (firstMedian + 1 < tupleCount) {
                            // second median is in the same frame
                            ftr.reset(fta, firstMedian + 1);
                        } else {
                            // second median is in the next frame
                            merger.nextFrame(frame);
                            fta.reset(frame.getBuffer());
                            ftr.reset(fta, 0);
                        }
                        medianVal =
                                (ADoubleSerializerDeserializer.getDouble(ftr.getFieldData(0), ftr.getFieldStart(0) + 1)
                                        + medianVal) / 2;
                    }
                    aDouble.setValue(medianVal);
                    found = true;
                    break;
                }
                currentTupleCount += tupleCount;
            }
            while (merger.nextFrame(frame)) {
                // consume the remaining frames to close the network channels gracefully
            }
        } finally {
            merger.close();
        }
        return found;
    }

    protected void setPartialResult(IPointable result, long fileId, String address, int port)
            throws HyracksDataException {
        try {
            resultStorage.reset();
            aInt64.setValue(count);
            longSerde.serialize(aInt64, resultStorage.getDataOutput());
            recBuilder.addField(COUNT_FIELD_ID, resultStorage);

            resultStorage.reset();
            aInt64.setValue(fileId);
            longSerde.serialize(aInt64, resultStorage.getDataOutput());
            recBuilder.addField(HANDLE_FIELD_ID, resultStorage);

            resultStorage.reset();
            aString.setValue(address);
            stringSerde.serialize(aString, resultStorage.getDataOutput());
            recBuilder.addField(ADDRESS_FIELD_ID, resultStorage);

            resultStorage.reset();
            aInt32.setValue(port);
            intSerde.serialize(aInt32, resultStorage.getDataOutput());
            recBuilder.addField(PORT_FIELD_ID, resultStorage);

            resultStorage.reset();
            recBuilder.write(resultStorage.getDataOutput(), true);
            result.set(resultStorage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected List<IFrame> getInFrames(int size, IHyracksTaskContext taskCtx) throws HyracksDataException {
        if (inFrames == null) {
            inFrames = new ArrayList<>(size);
        }
        int k = 0;
        for (int inFramesSize = inFrames.size(); k < size && k < inFramesSize; k++) {
            inFrames.get(k).reset();
        }
        for (; k < size; k++) {
            inFrames.add(new VSizeFrame(taskCtx));
        }
        return inFrames;
    }

    private IFrameReader createInputChannel(NetworkManager netManager, IHyracksTaskContext taskContext,
            NetworkAddress networkAddress, long jobId, long fileId) throws HyracksDataException {
        FileNetworkInputChannel FileNetworkInputChannel =
                new FileNetworkInputChannel(netManager, getSocketAddress(networkAddress), jobId, fileId);
        InputChannelFrameReader channelFrameReader = new InputChannelFrameReader(FileNetworkInputChannel);
        FileNetworkInputChannel.registerMonitor(channelFrameReader);
        FileNetworkInputChannel.open(taskContext);
        return channelFrameReader;
    }

    private static SocketAddress getSocketAddress(NetworkAddress netAddress) throws HyracksDataException {
        try {
            return new InetSocketAddress(InetAddress.getByAddress(netAddress.lookupIpAddress()), netAddress.getPort());
        } catch (UnknownHostException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static class PartialResult {

        long fileId;
        String address;
        int port;
        long count;

        PartialResult(long fileId, long count, String address, int port) {
            this.fileId = fileId;
            this.count = count;
            this.address = address;
            this.port = port;
        }
    }
}
