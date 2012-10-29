package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

@SuppressWarnings("rawtypes") 
public class DataGenUtils {
    public static IFieldValueGenerator getFieldGenFromSerde(ISerializerDeserializer serde, Random rnd, boolean sorted) {
        if (serde instanceof IntegerSerializerDeserializer) {
            if (sorted) {
                return new SortedIntegerFieldValueGenerator();
            } else {
                return new IntegerFieldValueGenerator(rnd);
            }
        } else if (serde instanceof FloatSerializerDeserializer) {
            if (sorted) {
                return new SortedFloatFieldValueGenerator();
            } else {
                return new FloatFieldValueGenerator(rnd);
            }
        } else if (serde instanceof DoubleSerializerDeserializer) {
            if (sorted) {
                return new SortedDoubleFieldValueGenerator();
            } else {
                return new DoubleFieldValueGenerator(rnd);
            }
        } else if (serde instanceof UTF8StringSerializerDeserializer) {
            return new StringFieldValueGenerator(20, rnd);
        }
        System.out.println("NULL");
        return null;
    }
    
    public static IFieldValueGenerator[] getFieldGensFromSerdes(ISerializerDeserializer[] serdes, Random rnd, boolean sorted) {
        IFieldValueGenerator[] fieldValueGens = new IFieldValueGenerator[serdes.length];
        for (int i = 0; i < serdes.length; i++) {
            fieldValueGens[i] = getFieldGenFromSerde(serdes[i], rnd, sorted);
        }
        return fieldValueGens;
    }
}
