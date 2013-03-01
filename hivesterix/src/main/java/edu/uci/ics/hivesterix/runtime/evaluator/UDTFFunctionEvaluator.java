package edu.uci.ics.hivesterix.runtime.evaluator;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import edu.uci.ics.hivesterix.logical.expression.Schema;
import edu.uci.ics.hivesterix.serde.lazy.LazyColumnar;
import edu.uci.ics.hivesterix.serde.lazy.LazyFactory;
import edu.uci.ics.hivesterix.serde.lazy.LazyObject;
import edu.uci.ics.hivesterix.serde.lazy.LazySerDe;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class UDTFFunctionEvaluator implements ICopyUnnestingFunction, Collector {

	/**
	 * udtf function
	 */
	private UDTFDesc func;

	/**
	 * input object inspector
	 */
	private ObjectInspector inputInspector;

	/**
	 * output object inspector
	 */
	private ObjectInspector outputInspector;

	/**
	 * object inspector for udtf
	 */
	private ObjectInspector[] udtfInputOIs;

	/**
	 * generic udtf
	 */
	private GenericUDTF udtf;

	/**
	 * data output
	 */
	private DataOutput out;

	/**
	 * the input row object
	 */
	private LazyColumnar cachedRowObject;

	/**
	 * cached row object (input)
	 */
	private Object[] cachedInputObjects;

	/**
	 * serialization/deserialization
	 */
	private SerDe lazySerDe;

	/**
	 * columns feed into UDTF
	 */
	private int[] columns;

	public UDTFFunctionEvaluator(UDTFDesc desc, Schema schema, int[] cols,
			DataOutput output) {
		this.func = desc;
		this.inputInspector = schema.toObjectInspector();
		udtf = func.getGenericUDTF();
		out = output;
		columns = cols;
	}

	@Override
	public void init(IFrameTupleReference tuple) throws AlgebricksException {
		cachedInputObjects = new LazyObject[columns.length];
		try {
			cachedRowObject = (LazyColumnar) LazyFactory
					.createLazyObject(inputInspector);
			outputInspector = udtf.initialize(udtfInputOIs);
		} catch (HiveException e) {
			throw new AlgebricksException(e);
		}
		udtf.setCollector(this);
		lazySerDe = new LazySerDe();
		readIntoCache(tuple);
	}

	@Override
	public boolean step() throws AlgebricksException {
		try {
			udtf.process(cachedInputObjects);
			return true;
		} catch (HiveException e) {
			throw new AlgebricksException(e);
		}
	}

	/**
	 * bind the tuple reference to the cached row object
	 * 
	 * @param r
	 */
	private void readIntoCache(IFrameTupleReference r) {
		cachedRowObject.init(r);
		for (int i = 0; i < cachedInputObjects.length; i++) {
			cachedInputObjects[i] = cachedRowObject.getField(columns[i]);
		}
	}

	/**
	 * serialize the result
	 * 
	 * @param result
	 *            the evaluation result
	 * @throws IOException
	 * @throws AlgebricksException
	 */
	private void serializeResult(Object result) throws SerDeException,
			IOException {
		BytesWritable outputWritable = (BytesWritable) lazySerDe.serialize(
				result, outputInspector);
		out.write(outputWritable.getBytes(), 0, outputWritable.getLength());
	}

	@Override
	public void collect(Object input) throws HiveException {
		try {
			serializeResult(input);
		} catch (IOException e) {
			throw new HiveException(e);
		} catch (SerDeException e) {
			throw new HiveException(e);
		}
	}
}
