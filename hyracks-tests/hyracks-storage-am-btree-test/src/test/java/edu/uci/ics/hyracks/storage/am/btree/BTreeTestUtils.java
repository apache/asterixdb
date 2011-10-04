package edu.uci.ics.hyracks.storage.am.btree;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeTestUtils {
    public static void createIntTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple, final int... fields)
            throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (final int i : fields) {
            IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }
    public static void createStringTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple, final String... fields)
            throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (final String s : fields) {
            UTF8StringSerializerDeserializer.INSTANCE.serialize(s, dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }
    
    public static BTree createBTree(IBufferCache bufferCache, int btreeFileId, ITypeTrait[] typeTraits, IBinaryComparator[] cmps) {
        MultiComparator cmp = new MultiComparator(typeTraits, cmps);
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);        
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, btreeFileId, 0, metaFrameFactory);
        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmp);
        return btree;
    }
    
    @SuppressWarnings("rawtypes") 
    public static ITypeTrait[] serdesToTypeTraits(ISerializerDeserializer[] serdes) {
        ITypeTrait[] typeTraits = new ITypeTrait[serdes.length];
        for (int i = 0; i < serdes.length; i++) {
            ISerializerDeserializer serde = serdes[i];
            if (serde instanceof IntegerSerializerDeserializer) {
                typeTraits[i] = ITypeTrait.INTEGER_TYPE_TRAIT;
                continue;
            }
            if (serde instanceof Integer64SerializerDeserializer) {
                typeTraits[i] = ITypeTrait.INTEGER64_TYPE_TRAIT;
                continue;
            }
            if (serde instanceof FloatSerializerDeserializer) {
                typeTraits[i] = ITypeTrait.FLOAT_TYPE_TRAIT;
                continue;
            }
            if (serde instanceof DoubleSerializerDeserializer) {
                typeTraits[i] = ITypeTrait.DOUBLE_TYPE_TRAIT;
                continue;
            }
            if (serde instanceof BooleanSerializerDeserializer) {
                typeTraits[i] = ITypeTrait.BOOLEAN_TYPE_TRAIT;
                continue;
            }
            typeTraits[i] = ITypeTrait.VARLEN_TYPE_TRAIT;
        }
        return typeTraits;
    }
}
