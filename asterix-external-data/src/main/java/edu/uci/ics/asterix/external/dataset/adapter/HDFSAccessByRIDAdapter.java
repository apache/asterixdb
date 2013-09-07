/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.ControlledADMTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.ControlledDelimitedDataTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.ControlledTupleParser;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Provides functionality for fetching specific external data records stored in an HDFS instance
 * using their RID.
 */
@SuppressWarnings({ "deprecation" })
public class HDFSAccessByRIDAdapter extends FileSystemBasedAdapter implements IControlledAdapter{

	private static final long serialVersionUID = 1L;
	private boolean newFrame;
	private transient ByteBuffer frameBuffer;
	private String inputFormat;
	private Configuration conf;
	private transient FileSystem fs;
	private RecordDescriptor inRecDesc;
	private final HashMap<Integer, String> files;

	public HDFSAccessByRIDAdapter(IAType atype, String inputFormat, AlgebricksPartitionConstraint clusterLocations, RecordDescriptor inRecDesc, Configuration conf, HashMap<Integer,String> files) {
		super(atype);
		this.inputFormat = inputFormat;
		this.conf = conf;
		this.inRecDesc = inRecDesc;
		this.files = files;
	}

	@Override
	public void configure(Map<String, Object> arguments) throws Exception {
		this.configuration = arguments;
		fs = FileSystem.get(conf);
		String specifiedFormat = (String) configuration.get(KEY_FORMAT);
		if (specifiedFormat == null) {
			throw new IllegalArgumentException(" Unspecified data format");
		} else if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(specifiedFormat)) {
			parserFactory = getDelimitedDataTupleParserFactory((ARecordType) atype);
		} else if (FORMAT_ADM.equalsIgnoreCase((String)configuration.get(KEY_FORMAT))) {
			parserFactory = new ControlledADMTupleParserFactory((ARecordType) atype);
		} else {
			throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT) + " not supported");
		}
	}

	@Override
	protected ITupleParserFactory getDelimitedDataTupleParserFactory(ARecordType recordType) throws AsterixException {
		int n = recordType.getFieldTypes().length;
		IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
		for (int i = 0; i < n; i++) {
			ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
			IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
			if (vpf == null) {
				throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
			}
			fieldParserFactories[i] = vpf;
		}
		String delimiterValue = (String) configuration.get(KEY_DELIMITER);
		if (delimiterValue != null && delimiterValue.length() > 1) {
			throw new AsterixException("improper delimiter");
		}

		Character delimiter = delimiterValue.charAt(0);
		return new ControlledDelimitedDataTupleParserFactory(recordType, fieldParserFactories, delimiter);
	}

	@Override
	public void start(int partition, IFrameWriter writer) throws Exception {
		throw new NotImplementedException("Access by RID adapter doesn't support start function");
	}

	public void processNextFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException
	{
		frameBuffer = buffer;
		newFrame = true;
		((ControlledTupleParser)parser).parseNext(writer);
	}

	public void close(IFrameWriter writer) throws HyracksDataException
	{
		((ControlledTupleParser)parser).close(writer);
	}

	public AdapterType getAdapterType() {
		return AdapterType.READ;
	}

	@Override
	public void initialize(IHyracksTaskContext ctx) throws Exception {
		this.ctx = ctx;
		//create parser and initialize it with an instance of the inputStream
		parser = parserFactory.createTupleParser(ctx);
		((ControlledTupleParser)parser).initialize(getInputStream(0));
	}

	@Override
	public InputStream getInputStream(int partition) throws IOException {

		//if files map is not null, then it is optimized and we should return optimized inputStream, else return regular
		if(files == null)
		{	

			//different input stream implementation based on the input format
			if(inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_RC))
			{
				return new InputStream() {
					private RCFile.Reader reader;
					private int rowDifference;
					private String lastFileName = "";
					private String newFileName;
					private long lastByteLocation = 0;
					private long newByteLocation = 0;
					private int lastRowNumber = 0;
					private int newRowNumber = 0;
					private LongWritable key;
					private BytesRefArrayWritable value;
					private int EOL = "\n".getBytes()[0];
					private byte delimiter = 0x01;
					private boolean pendingValue = false;
					private int currentTupleIdx;
					private int numberOfTuplesInCurrentFrame;
					private IFrameTupleAccessor tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),inRecDesc);
					private ByteBufferInputStream bbis = new ByteBufferInputStream();
					private DataInputStream dis = new DataInputStream(bbis);

					@Override
					public void close()
					{
						if (reader != null)
						{
							reader.close();
						}
						try {
							super.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {
						if(newFrame)
						{
							//first time called with this frame
							//reset frame buffer
							tupleAccessor.reset(frameBuffer);
							//get number of tuples in frame
							numberOfTuplesInCurrentFrame = tupleAccessor.getTupleCount();
							//set tuple index to first tuple
							currentTupleIdx = 0;
							//set new frame to false
							newFrame = false;
							pendingValue = false;
						}

						//check and see if there is a pending value
						//Double check this
						int numBytes = 0;
						if (pendingValue) {
							//last value didn't fit into buffer
							int sizeOfNextTuple = getTupleSize(value) + 1;
							if(sizeOfNextTuple > len)
							{
								return 0;
							}
							copyCurrentTuple(buffer, offset + numBytes);
							buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
							numBytes += sizeOfNextTuple;
							//set pending to false
							pendingValue = false;
							//move to next tuple
							currentTupleIdx++;
						}

						//No pending value or done with pending value
						//check if there are more tuples in the frame
						while(currentTupleIdx < numberOfTuplesInCurrentFrame)
						{
							//get 3 things from the current tuple in the frame(File name, byte location and row number)
							//get the fileName
							bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 0));
							newFileName = ((AString) inRecDesc.getFields()[0].deserialize(dis)).getStringValue();
							//check if it is a new file
							if(!lastFileName.equals(newFileName))//stringBuilder.toString()))
							{
								//new file
								lastFileName = newFileName;
								//close old file
								if(reader != null)
								{
									reader.close();
								}
								//open new file
								reader = new Reader(fs, new Path(lastFileName), conf);
								//read and save byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								lastByteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(lastByteLocation);
								//read and save rowNumber
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 2));
								lastRowNumber = ((AInt32)(inRecDesc.getFields()[2].deserialize(dis))).getIntegerValue();
								//loop until row
								for(int i=0; i < lastRowNumber; i++)
								{
									//this loop perform a single I/O and move to the next record in the block which is already in memory
									//if no more records in the current block, it perform another I/O and get the next block
									//<this should never happen here>
									reader.next(key);
								}
								//read record
								reader.getCurrentRow(value);
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = getTupleSize(value) + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = true;
									return numBytes;
								}
								copyCurrentTuple(buffer, offset + numBytes);
								buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							else
							{
								//same file
								//get the byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								newByteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();

								//check if same block
								if(lastByteLocation != newByteLocation)
								{
									//new block
									lastByteLocation = newByteLocation;
									//seek
									reader.seek(lastByteLocation);
									//read and save rowNumber
									bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 2));
									lastRowNumber = ((AInt32)(inRecDesc.getFields()[2].deserialize(dis))).getIntegerValue();
									//loop until row
									for(int i=0; i < lastRowNumber; i++)
									{
										reader.next(key);
									}
									//read record
									reader.getCurrentRow(value);
									//copy it to the buffer if there is enough space
									int sizeOfNextTuple = getTupleSize(value) + 1;
									if(sizeOfNextTuple + numBytes > len)
									{
										//mark waiting value
										pendingValue = true;
										return numBytes;
									}
									copyCurrentTuple(buffer, offset + numBytes);
									buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
									numBytes += sizeOfNextTuple;
								}
								else
								{
									//same block
									//get the row number
									bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 2));
									newRowNumber = ((AInt32)(inRecDesc.getFields()[2].deserialize(dis))).getIntegerValue();

									//calculate row difference
									rowDifference = newRowNumber - lastRowNumber;

									//update last row number
									lastRowNumber = newRowNumber;

									//move to the new row
									for(int i=0; i < rowDifference; i++)
									{
										reader.next(key);
									}
									//read record
									reader.getCurrentRow(value);

									//copy it to the buffer if there is enough space
									int sizeOfNextTuple = getTupleSize(value) + 1;
									if(sizeOfNextTuple + numBytes > len)
									{
										//mark waiting value
										pendingValue = true;
										return numBytes;
									}
									copyCurrentTuple(buffer, offset + numBytes);
									buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
									numBytes += sizeOfNextTuple;
								}
							}
							//move to next tuple
							currentTupleIdx++;
						}	
						//no more tuples in frame
						return (numBytes == 0) ? -1 : numBytes;
					}

					private void copyCurrentTuple(byte[] buffer, int offset) throws IOException {
						int rcOffset = 0;
						for(int i=0; i< value.size(); i++)
						{
							System.arraycopy(value.get(i).getData(), value.get(i).getStart(), buffer, offset + rcOffset, value.get(i).getLength());
							rcOffset += value.get(i).getLength() + 1;
							buffer[rcOffset - 1] = delimiter;
						}
					}

					private int getTupleSize(BytesRefArrayWritable value2) {
						int size=0;
						//loop over rc column and add lengths
						for(int i=0; i< value.size(); i++)
						{
							size += value.get(i).getLength();
						}
						//add delimeters bytes sizes
						size += value.size() -1;
						return size;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}
				};
			}
			else if (inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT))
			{
				return new InputStream() {
					private FSDataInputStream reader;
					private String lastFileName = "";
					private String newFileName;
					private int EOL = "\n".getBytes()[0];
					private int currentTupleIdx;
					private int numberOfTuplesInCurrentFrame;
					private long byteLocation;
					private IFrameTupleAccessor tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),inRecDesc);
					private String value;
					private String pendingValue = null;
					private ByteBufferInputStream bbis = new ByteBufferInputStream();
					private DataInputStream dis = new DataInputStream(bbis);

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {
						if(newFrame)
						{
							//first time called with this frame
							//reset frame buffer
							tupleAccessor.reset(frameBuffer);
							//get number of tuples in frame
							numberOfTuplesInCurrentFrame = tupleAccessor.getTupleCount();
							//set tuple index to first tuple
							currentTupleIdx = 0;
							//set new frame to false
							newFrame = false;
						}

						//check and see if there is a pending value
						int numBytes = 0;
						if (pendingValue != null) {
							//last value didn't fit into buffer
							int sizeOfNextTuple = pendingValue.length() + 1;
							if(sizeOfNextTuple > len)
							{
								return 0;
							}
							//there is enough space
							System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.length());
							buffer[offset + numBytes + pendingValue.length()] = (byte) EOL;
							numBytes += sizeOfNextTuple;
							//set pending to false
							pendingValue = null;
							//move to next tuple
							currentTupleIdx++;
						}

						//No pending value or done with pending value
						//check if there are more tuples in the frame
						while(currentTupleIdx < numberOfTuplesInCurrentFrame)
						{
							//get the fileName
							bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 0));
							newFileName = ((AString) inRecDesc.getFields()[0].deserialize(dis)).getStringValue();
							//check if it is a new file
							if(!lastFileName.equals(newFileName))
							{
								//new file
								lastFileName = newFileName;
								//close old file
								if(reader != null)
								{
									reader.close();
								}
								//open new file
								reader = fs.open(new Path(lastFileName));
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								value = reader.readLine();
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.length() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.length());
								buffer[offset + numBytes + value.length()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							else
							{
								//same file, just seek and read
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								value = reader.readLine();
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.length() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.length());
								buffer[offset + numBytes + value.length()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							currentTupleIdx++;
						}
						return (numBytes == 0) ? -1 : numBytes;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}

					@Override
					public void close(){
						try {
							if (reader != null)
							{
								reader.close();
							}
							super.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				};
			}
			else if (inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_SEQUENCE))
			{
				return new InputStream() {
					private SequenceFile.Reader reader;
					private Writable key;
					private Text value;
					private String lastFileName = "";
					private String newFileName;
					private long byteLocation;
					private int EOL = "\n".getBytes()[0];
					private int currentTupleIdx;
					private int numberOfTuplesInCurrentFrame;
					private IFrameTupleAccessor tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),inRecDesc);
					private Text pendingValue = null;
					private ByteBufferInputStream bbis = new ByteBufferInputStream();
					private DataInputStream dis = new DataInputStream(bbis);

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {

						if(newFrame)
						{
							//first time called with this frame
							//reset frame buffer
							tupleAccessor.reset(frameBuffer);
							//get number of tuples in frame
							numberOfTuplesInCurrentFrame = tupleAccessor.getTupleCount();
							//set tuple index to first tuple
							currentTupleIdx = 0;
							//set new frame to false
							newFrame = false;
						}

						//check and see if there is a pending value
						//Double check this
						int numBytes = 0;
						if (pendingValue != null) {
							//last value didn't fit into buffer
							int sizeOfNextTuple = pendingValue.getLength() + 1;
							if(sizeOfNextTuple > len)
							{
								return 0;
							}
							//there is enough space
							System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.getLength());
							buffer[offset + numBytes + pendingValue.getLength()] = (byte) EOL;
							numBytes += sizeOfNextTuple;
							//set pending to false
							pendingValue = null;
							//move to next tuple
							currentTupleIdx++;
						}

						//No pending value or done with pending value
						//check if there are more tuples in the frame
						while(currentTupleIdx < numberOfTuplesInCurrentFrame)
						{
							//get the fileName]
							bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 0));
							newFileName = ((AString) inRecDesc.getFields()[0].deserialize(dis)).getStringValue();
							//check if it is a new file
							if(!lastFileName.equals(newFileName))
							{
								//new file
								lastFileName = newFileName;
								//close old file
								if(reader != null)
								{
									reader.close();
								}
								//open new file
								reader = new SequenceFile.Reader(fs,new Path(lastFileName),conf);
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								reader.next(key, value);
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.getLength() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
								buffer[offset + numBytes + value.getLength()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							else
							{
								//same file, just seek and read
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								reader.next(key, value);
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.getLength() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
								buffer[offset + numBytes + value.getLength()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							currentTupleIdx++;
						}
						return (numBytes == 0) ? -1 : numBytes;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}

					@Override
					public void close(){
						try {
							if (reader != null)
							{
								reader.close();
							}
							super.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				};
			}
			//unknow format
			throw new IOException("Unknown input format");
		}
		else
		{
			//optimized
			//different input stream implementation based on the input format
			if(inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_RC))
			{
				return new InputStream() {
					private RCFile.Reader reader;
					private int rowDifference;
					private int lastFileNumber = -1;
					private int newFileNumber = 0;
					private long lastByteLocation = 0;
					private long newByteLocation = 0;
					private int lastRowNumber = 0;
					private int newRowNumber = 0;
					private LongWritable key;
					private BytesRefArrayWritable value;
					private int EOL = "\n".getBytes()[0];
					private byte delimiter = 0x01;
					private boolean pendingValue = false;
					private int currentTupleIdx;
					private int numberOfTuplesInCurrentFrame;
					private IFrameTupleAccessor tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),inRecDesc);
					private ByteBufferInputStream bbis = new ByteBufferInputStream();
					private DataInputStream dis = new DataInputStream(bbis);

					@Override
					public void close()
					{
						if (reader != null)
						{
							reader.close();
						}
						try {
							super.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {
						if(newFrame)
						{
							//first time called with this frame
							//reset frame buffer
							tupleAccessor.reset(frameBuffer);
							//get number of tuples in frame
							numberOfTuplesInCurrentFrame = tupleAccessor.getTupleCount();
							//set tuple index to first tuple
							currentTupleIdx = 0;
							//set new frame to false
							newFrame = false;
							pendingValue = false;
						}

						//check and see if there is a pending value
						//Double check this
						int numBytes = 0;
						if (pendingValue) {
							//last value didn't fit into buffer
							int sizeOfNextTuple = getTupleSize(value) + 1;
							if(sizeOfNextTuple > len)
							{
								return 0;
							}
							copyCurrentTuple(buffer, offset + numBytes);
							buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
							numBytes += sizeOfNextTuple;
							//set pending to false
							pendingValue = false;
							//move to next tuple
							currentTupleIdx++;
						}

						//No pending value or done with pending value
						//check if there are more tuples in the frame
						while(currentTupleIdx < numberOfTuplesInCurrentFrame)
						{
							//get 3 things from the current tuple in the frame(File name, byte location and row number)
							//get the fileName
							bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 0));
							newFileNumber = ((AInt32) inRecDesc.getFields()[0].deserialize(dis)).getIntegerValue();
							//check if it is a new file
							if(lastFileNumber != newFileNumber)
							{
								//new file
								lastFileNumber = newFileNumber;
								//close old file
								if(reader != null)
								{
									reader.close();
								}
								//open new file
								reader = new Reader(fs, new Path(files.get(newFileNumber)), conf);
								//read and save byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								lastByteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(lastByteLocation);
								//read and save rowNumber
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 2));
								lastRowNumber = ((AInt32)(inRecDesc.getFields()[2].deserialize(dis))).getIntegerValue();
								//loop until row
								for(int i=0; i < lastRowNumber; i++)
								{
									//this loop perform a single I/O and move to the next record in the block which is already in memory
									//if no more records in the current block, it perform another I/O and get the next block
									//<this should never happen here>
									reader.next(key);
								}
								//read record
								reader.getCurrentRow(value);
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = getTupleSize(value) + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = true;
									return numBytes;
								}
								copyCurrentTuple(buffer, offset + numBytes);
								buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							else
							{
								//same file
								//get the byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								newByteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();

								//check if same block
								if(lastByteLocation != newByteLocation)
								{
									//new block
									lastByteLocation = newByteLocation;
									//seek
									reader.seek(lastByteLocation);
									//read and save rowNumber
									bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 2));
									lastRowNumber = ((AInt32)(inRecDesc.getFields()[2].deserialize(dis))).getIntegerValue();
									//loop until row
									for(int i=0; i < lastRowNumber; i++)
									{
										reader.next(key);
									}
									//read record
									reader.getCurrentRow(value);
									//copy it to the buffer if there is enough space
									int sizeOfNextTuple = getTupleSize(value) + 1;
									if(sizeOfNextTuple + numBytes > len)
									{
										//mark waiting value
										pendingValue = true;
										return numBytes;
									}
									copyCurrentTuple(buffer, offset + numBytes);
									buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
									numBytes += sizeOfNextTuple;
								}
								else
								{
									//same block
									//get the row number
									bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 2));
									newRowNumber = ((AInt32)(inRecDesc.getFields()[2].deserialize(dis))).getIntegerValue();

									//calculate row difference
									rowDifference = newRowNumber - lastRowNumber;

									//update last row number
									lastRowNumber = newRowNumber;

									//move to the new row
									for(int i=0; i < rowDifference; i++)
									{
										reader.next(key);
									}
									//read record
									reader.getCurrentRow(value);

									//copy it to the buffer if there is enough space
									int sizeOfNextTuple = getTupleSize(value) + 1;
									if(sizeOfNextTuple + numBytes > len)
									{
										//mark waiting value
										pendingValue = true;
										return numBytes;
									}
									copyCurrentTuple(buffer, offset + numBytes);
									buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
									numBytes += sizeOfNextTuple;
								}
							}
							//move to next tuple
							currentTupleIdx++;
						}	
						//no more tuples in frame
						return (numBytes == 0) ? -1 : numBytes;
					}

					private void copyCurrentTuple(byte[] buffer, int offset) throws IOException {
						int rcOffset = 0;
						for(int i=0; i< value.size(); i++)
						{
							System.arraycopy(value.get(i).getData(), value.get(i).getStart(), buffer, offset + rcOffset, value.get(i).getLength());
							rcOffset += value.get(i).getLength() + 1;
							buffer[rcOffset - 1] = delimiter;
						}
					}

					private int getTupleSize(BytesRefArrayWritable value2) {
						int size=0;
						//loop over rc column and add lengths
						for(int i=0; i< value.size(); i++)
						{
							size += value.get(i).getLength();
						}
						//add delimeters bytes sizes
						size += value.size() -1;
						return size;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}
				};
			}
			else if (inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT))
			{
				return new InputStream() {
					private FSDataInputStream reader;
					private int lastFileNumber = -1;
					private int newFileNumber = 0;
					private int EOL = "\n".getBytes()[0];
					private int currentTupleIdx;
					private int numberOfTuplesInCurrentFrame;
					private long byteLocation;
					private IFrameTupleAccessor tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),inRecDesc);
					private String value;
					private String pendingValue = null;
					private ByteBufferInputStream bbis = new ByteBufferInputStream();
					private DataInputStream dis = new DataInputStream(bbis);

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {
						if(newFrame)
						{
							//first time called with this frame
							//reset frame buffer
							tupleAccessor.reset(frameBuffer);
							//get number of tuples in frame
							numberOfTuplesInCurrentFrame = tupleAccessor.getTupleCount();
							//set tuple index to first tuple
							currentTupleIdx = 0;
							//set new frame to false
							newFrame = false;
						}

						//check and see if there is a pending value
						int numBytes = 0;
						if (pendingValue != null) {
							//last value didn't fit into buffer
							int sizeOfNextTuple = pendingValue.length() + 1;
							if(sizeOfNextTuple > len)
							{
								return 0;
							}
							//there is enough space
							System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.length());
							buffer[offset + numBytes + pendingValue.length()] = (byte) EOL;
							numBytes += sizeOfNextTuple;
							//set pending to false
							pendingValue = null;
							//move to next tuple
							currentTupleIdx++;
						}

						//No pending value or done with pending value
						//check if there are more tuples in the frame
						while(currentTupleIdx < numberOfTuplesInCurrentFrame)
						{
							//get the file number
							bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 0));
							newFileNumber = ((AInt32) inRecDesc.getFields()[0].deserialize(dis)).getIntegerValue();
							//check if it is a new file
							if(lastFileNumber != newFileNumber)
							{
								//new file
								lastFileNumber = newFileNumber;
								//close old file
								if(reader != null)
								{
									reader.close();
								}
								
								//open new file
								reader = fs.open(new Path(files.get(newFileNumber)));
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								value = reader.readLine();
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.length() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.length());
								buffer[offset + numBytes + value.length()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							else
							{
								//same file, just seek and read
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								value = reader.readLine();
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.length() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.length());
								buffer[offset + numBytes + value.length()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							currentTupleIdx++;
						}
						return (numBytes == 0) ? -1 : numBytes;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}

					@Override
					public void close(){
						try {
							if (reader != null)
							{
								reader.close();
							}
							super.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				};
			}
			else if (inputFormat.equals(HDFSAdapterFactory.INPUT_FORMAT_SEQUENCE))
			{
				return new InputStream() {
					private SequenceFile.Reader reader;
					private Writable key;
					private Text value;
					private int lastFileNumber = -1;
					private int newFileNumber = 0;
					private long byteLocation;
					private int EOL = "\n".getBytes()[0];
					private int currentTupleIdx;
					private int numberOfTuplesInCurrentFrame;
					private IFrameTupleAccessor tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),inRecDesc);
					private Text pendingValue = null;
					private ByteBufferInputStream bbis = new ByteBufferInputStream();
					private DataInputStream dis = new DataInputStream(bbis);

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {

						if(newFrame)
						{
							//first time called with this frame
							//reset frame buffer
							tupleAccessor.reset(frameBuffer);
							//get number of tuples in frame
							numberOfTuplesInCurrentFrame = tupleAccessor.getTupleCount();
							//set tuple index to first tuple
							currentTupleIdx = 0;
							//set new frame to false
							newFrame = false;
						}

						//check and see if there is a pending value
						//Double check this
						int numBytes = 0;
						if (pendingValue != null) {
							//last value didn't fit into buffer
							int sizeOfNextTuple = pendingValue.getLength() + 1;
							if(sizeOfNextTuple > len)
							{
								return 0;
							}
							//there is enough space
							System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.getLength());
							buffer[offset + numBytes + pendingValue.getLength()] = (byte) EOL;
							numBytes += sizeOfNextTuple;
							//set pending to false
							pendingValue = null;
							//move to next tuple
							currentTupleIdx++;
						}

						//No pending value or done with pending value
						//check if there are more tuples in the frame
						while(currentTupleIdx < numberOfTuplesInCurrentFrame)
						{
							//get the fileName]
							bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 0));
							newFileNumber = ((AInt32) inRecDesc.getFields()[0].deserialize(dis)).getIntegerValue();
							//check if it is a new file
							if(lastFileNumber != newFileNumber)
							{
								//new file
								lastFileNumber = newFileNumber;
								//close old file
								if(reader != null)
								{
									reader.close();
								}
								//open new file
								reader = new SequenceFile.Reader(fs,new Path(files.get(newFileNumber)),conf);
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								reader.next(key, value);
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.getLength() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
								buffer[offset + numBytes + value.getLength()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							else
							{
								//same file, just seek and read
								//read byte location
								bbis.setByteBuffer(frameBuffer, tupleAccessor.getTupleStartOffset(currentTupleIdx) + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(currentTupleIdx, 1));
								byteLocation = ((AInt64) inRecDesc.getFields()[1].deserialize(dis)).getLongValue();
								//seek
								reader.seek(byteLocation);
								//read record
								reader.next(key, value);
								//copy it to the buffer if there is enough space
								int sizeOfNextTuple = value.getLength() + 1;
								if(sizeOfNextTuple + numBytes > len)
								{
									//mark waiting value
									pendingValue = value;
									return numBytes;
								}
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
								buffer[offset + numBytes + value.getLength()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
							currentTupleIdx++;
						}
						return (numBytes == 0) ? -1 : numBytes;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}

					@Override
					public void close(){
						try {
							if (reader != null)
							{
								reader.close();
							}
							super.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				};
			}
			//unknow format
			throw new IOException("Unknown input format");
		}
	}

	@Override
	public AlgebricksPartitionConstraint getPartitionConstraint()
			throws Exception {
		return partitionConstraint;
	}
}