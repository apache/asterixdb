package edu.uci.ics.pregelix.api.graph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public interface GlobalAggregator<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable, T extends Writable> {
	/**
	 * initialize combiner
	 */
	public void init();

	/**
	 * step call
	 * 
	 * @param vertexIndex
	 * @param msg
	 * @throws IOException
	 */
	public void step(Vertex<I, V, E, M> v) throws IOException;

	/**
	 * finish aggregate
	 * 
	 * @return the final aggregate value
	 */
	public T finish();
}
