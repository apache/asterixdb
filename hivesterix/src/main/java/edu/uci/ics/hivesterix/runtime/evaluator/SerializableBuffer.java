package edu.uci.ics.hivesterix.runtime.evaluator;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

public interface SerializableBuffer extends AggregationBuffer {

	public void deSerializeAggBuffer(byte[] data, int start, int len);

	public void serializeAggBuffer(byte[] data, int start, int len);

	public void serializeAggBuffer(DataOutput output) throws IOException;

}
