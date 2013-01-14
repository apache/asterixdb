package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hivesterix.serde.parser.IHiveParser;
import edu.uci.ics.hivesterix.serde.parser.TextToBinaryTupleParser;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class HiveTupleParser extends AbstractHiveTupleParser {

	private int[] outputColumnsOffset;
	/**
	 * class of input format
	 */
	private InputFormat inputFormat;

	/**
	 * serialization/deserialization object
	 */
	private SerDe serDe;

	/**
	 * the input row object inspector
	 */
	private ObjectInspector objectInspector;

	/**
	 * the hadoop job conf
	 */
	private JobConf job;

	/**
	 * Hyrax context to control resource allocation
	 */
	private final IHyracksTaskContext ctx;

	/**
	 * lazy serde: format flow in between operators
	 */
	private final SerDe outputSerDe;

	/**
	 * the parser from hive data to binary data
	 */
	private IHiveParser parser = null;

	/**
	 * parser for any hive input format
	 * 
	 * @param inputFormatClass
	 * @param serDeClass
	 * @param tbl
	 * @param conf
	 * @throws AlgebricksException
	 */
	public HiveTupleParser(String inputFormatClass, String serDeClass,
			String outputSerDeClass, Properties tbl, JobConf conf,
			final IHyracksTaskContext ctx, int[] outputColumnsOffset)
			throws AlgebricksException {
		try {
			conf.setClassLoader(this.getClass().getClassLoader());

			inputFormat = (InputFormat) ReflectionUtils.newInstance(
					Class.forName(inputFormatClass), conf);
			job = conf;

			// initialize the input serde
			serDe = (SerDe) ReflectionUtils.newInstance(
					Class.forName(serDeClass), job);
			serDe.initialize(job, tbl);

			// initialize the output serde
			outputSerDe = (SerDe) ReflectionUtils.newInstance(
					Class.forName(outputSerDeClass), job);
			outputSerDe.initialize(job, tbl);

			// object inspector of the row
			objectInspector = serDe.getObjectInspector();

			// hyracks context
			this.ctx = ctx;
			this.outputColumnsOffset = outputColumnsOffset;

			if (objectInspector instanceof LazySimpleStructObjectInspector) {
				LazySimpleStructObjectInspector rowInspector = (LazySimpleStructObjectInspector) objectInspector;
				List<? extends StructField> fieldRefs = rowInspector
						.getAllStructFieldRefs();
				boolean lightWeightParsable = true;
				for (StructField fieldRef : fieldRefs) {
					Category category = fieldRef.getFieldObjectInspector()
							.getCategory();
					if (!(category == Category.PRIMITIVE)) {
						lightWeightParsable = false;
						break;
					}
				}
				if (lightWeightParsable)
					parser = new TextToBinaryTupleParser(
							this.outputColumnsOffset, this.objectInspector);
			}
		} catch (Exception e) {
			throw new AlgebricksException(e);
		}
	}

	/**
	 * parse a input HDFS file split, the result is send to the writer
	 * one-frame-a-time
	 * 
	 * @param split
	 *            the HDFS file split
	 * @param writer
	 *            the writer
	 * @throws HyracksDataException
	 *             if there is sth. wrong in the ser/de
	 */
	@Override
	public void parse(FileSplit split, IFrameWriter writer)
			throws HyracksDataException {
		try {
			StructObjectInspector structInspector = (StructObjectInspector) objectInspector;

			// create the reader, key, and value
			RecordReader reader = inputFormat.getRecordReader(split, job,
					Reporter.NULL);
			Object key = reader.createKey();
			Object value = reader.createValue();

			// allocate a new frame
			ByteBuffer frame = ctx.allocateFrame();
			FrameTupleAppender appender = new FrameTupleAppender(
					ctx.getFrameSize());
			appender.reset(frame, true);

			List<? extends StructField> fieldRefs = structInspector
					.getAllStructFieldRefs();
			int size = 0;
			for (int i = 0; i < outputColumnsOffset.length; i++)
				if (outputColumnsOffset[i] >= 0)
					size++;

			ArrayTupleBuilder tb = new ArrayTupleBuilder(size);
			DataOutput dos = tb.getDataOutput();
			StructField[] outputFieldRefs = new StructField[size];
			Object[] outputFields = new Object[size];
			for (int i = 0; i < outputColumnsOffset.length; i++)
				if (outputColumnsOffset[i] >= 0)
					outputFieldRefs[outputColumnsOffset[i]] = fieldRefs.get(i);

			long serDeTime = 0;
			while (reader.next(key, value)) {
				long start = System.currentTimeMillis();
				// reuse the tuple builder
				tb.reset();
				if (parser != null) {
					Text text = (Text) value;
					parser.parse(text.getBytes(), 0, text.getLength(), tb);
				} else {
					Object row = serDe.deserialize((Writable) value);
					// write fields to the tuple builder one by one
					int i = 0;
					for (StructField fieldRef : fieldRefs) {
						if (outputColumnsOffset[i] >= 0)
							outputFields[outputColumnsOffset[i]] = structInspector
									.getStructFieldData(row, fieldRef);
						i++;
					}

					i = 0;
					for (Object field : outputFields) {
						BytesWritable fieldWritable = (BytesWritable) outputSerDe
								.serialize(field, outputFieldRefs[i]
										.getFieldObjectInspector());
						dos.write(fieldWritable.getBytes(), 0,
								fieldWritable.getSize());
						tb.addFieldEndOffset();
						i++;
					}
				}
				long end = System.currentTimeMillis();
				serDeTime += (end - start);

				if (!appender.append(tb.getFieldEndOffsets(),
						tb.getByteArray(), 0, tb.getSize())) {
					if (appender.getTupleCount() <= 0)
						throw new IllegalStateException(
								"zero tuples in a frame!");
					FrameUtils.flushFrame(frame, writer);
					appender.reset(frame, true);
					if (!appender.append(tb.getFieldEndOffsets(),
							tb.getByteArray(), 0, tb.getSize())) {
						throw new IllegalStateException();
					}
				}
			}
			System.out.println("serde time: " + serDeTime);
			reader.close();
			System.gc();

			// flush the last frame
			if (appender.getTupleCount() > 0) {
				FrameUtils.flushFrame(frame, writer);
			}
		} catch (IOException e) {
			throw new HyracksDataException(e);
		} catch (SerDeException e) {
			throw new HyracksDataException(e);
		}
	}
}
