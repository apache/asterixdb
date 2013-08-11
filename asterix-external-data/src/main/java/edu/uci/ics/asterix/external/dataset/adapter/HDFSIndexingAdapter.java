package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.log4j.Logger;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;


/**
 * Provides functionality for reading external files and attach RID info to them before they are sent to the parser
 * 
 *  Room for optimization: get numbers of indexing fields (or fields names for adm) and do a quick filtering before sending to parser
 *  
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class HDFSIndexingAdapter extends FileSystemBasedAdapter {

	private static final long serialVersionUID = 1L;
	public static final Logger LOGGER = Logger.getLogger(HDFSIndexingAdapter.class.getName());
	private transient String[] readSchedule;
	private transient boolean executed[];
	private transient InputSplit[] inputSplits;
	private transient JobConf conf;
	private transient AlgebricksPartitionConstraint clusterLocations;

	private transient String nodeName;

	public static final byte[] fileNameFieldNameWithRecOpeningBraces = "{\"_file-name\":\"".getBytes();
	public static final byte[] bytelocationFieldName = ",\"_byte-location\":".getBytes();
	public static final byte[] bytelocationValueEnd = "i64,".getBytes();

	public HDFSIndexingAdapter(IAType atype, String[] readSchedule, boolean[] executed, InputSplit[] inputSplits, JobConf conf,
			AlgebricksPartitionConstraint clusterLocations) {
		super(atype);
		this.readSchedule = readSchedule;
		this.executed = executed;
		this.inputSplits = inputSplits;
		this.conf = conf;
		this.clusterLocations = clusterLocations;
	}

	@Override
	public void configure(Map<String, Object> arguments) throws Exception {
		LOGGER.info("Configuring the adapter, why does it disappear");
		this.configuration = arguments;
		LOGGER.info("Configuring format");
		configureFormat();
	}

	public AdapterType getAdapterType() {
		return AdapterType.READ;
	}

	@Override
	public void initialize(IHyracksTaskContext ctx) throws Exception {
		this.ctx = ctx;
		this.nodeName = ctx.getJobletContext().getApplicationContext().getNodeId();
	}

	private Reporter getReporter() {
		Reporter reporter = new Reporter() {

			@Override
			public Counter getCounter(Enum<?> arg0) {
				return null;
			}

			@Override
			public Counter getCounter(String arg0, String arg1) {
				return null;
			}

			@Override
			public InputSplit getInputSplit() throws UnsupportedOperationException {
				return null;
			}

			@Override
			public void incrCounter(Enum<?> arg0, long arg1) {
			}

			@Override
			public void incrCounter(String arg0, String arg1, long arg2) {
			}

			@Override
			public void setStatus(String arg0) {
			}

			@Override
			public void progress() {
			}
		};

		return reporter;
	}

	@Override
	public InputStream getInputStream(int partition) throws IOException {
		LOGGER.info("Creating the input stream in node: "+ nodeName);
		//LOGGER.info("Number of input splits found = "+ inputSplits.length);
		if(conf.getInputFormat() instanceof RCFileInputFormat)
		{
			//indexing rc input format
			return new InputStream() {

				private RecordReader<LongWritable, BytesRefArrayWritable> reader;
				private LongWritable key;
				private BytesRefArrayWritable value;
				private boolean hasMore = false;
				private int EOL = "\n".getBytes()[0];
				private byte delimiter = 0x01;
				private boolean pendingValue = false;
				private int currentSplitIndex = 0;
				private byte[] fileName;
				private byte[] byteLocation;
				private byte[] rowNumberBytes;
				private long blockByteLocation;
				private long NextblockByteLocation;
				private int rowNumber;
				
				@SuppressWarnings("unchecked")
				private boolean moveToNext() throws IOException {
					for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
						/**
						 * read all the partitions scheduled to the current node
						 */
						if (readSchedule[currentSplitIndex].equals(nodeName)) {
							/**
							 * pick an unread split to read
							 * synchronize among simultaneous partitions in the same machine
							 */
							synchronized (executed) {
								if (executed[currentSplitIndex] == false) {
									executed[currentSplitIndex] = true;
								} else {
									continue;
								}
							}

							/**
							 * read the split
							 */
							reader = getRecordReader(currentSplitIndex);
							key = reader.createKey();
							value = reader.createValue();
							fileName = ((FileSplit)(inputSplits[currentSplitIndex])).getPath().toUri().getPath().getBytes();
							blockByteLocation = reader.getPos();
							pendingValue = reader.next(key, value);
							NextblockByteLocation = reader.getPos();
							rowNumber = 1;
							byteLocation = String.valueOf(blockByteLocation).getBytes("UTF-8");
							rowNumberBytes = String.valueOf(rowNumber).getBytes("UTF-8");
							return true;
						}
					}
					return false;
				}

				@Override
				public int read(byte[] buffer, int offset, int len) throws IOException {
					if (reader == null) {
						if (!moveToNext()) {
							//nothing to read
							return -1;
						}
					}

					int numBytes = 0;
					if (pendingValue) {
						//last value didn't fit into buffer
						// 1 for EOL
						int sizeOfNextTuple = getTupleSize(value) + 1;
						//fileName.length + byteLocation.length + rowNumberBytes.length;

						//copy filename
						System.arraycopy(fileName, 0, buffer, offset + numBytes, fileName.length);
						buffer[offset + numBytes + fileName.length] = delimiter;
						numBytes += fileName.length + 1;

						//copy byte location
						System.arraycopy(byteLocation, 0, buffer, offset + numBytes, byteLocation.length);
						buffer[offset + numBytes + byteLocation.length] = delimiter;
						numBytes += byteLocation.length + 1;

						//copy row number
						System.arraycopy(rowNumberBytes, 0, buffer, offset + numBytes, rowNumberBytes.length);
						buffer[offset + numBytes + rowNumberBytes.length] = delimiter;
						numBytes += rowNumberBytes.length + 1;

						copyCurrentTuple(buffer, offset + numBytes);
						buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
						numBytes += sizeOfNextTuple;
						//set pending to false
						pendingValue = false;
					}

					while (numBytes < len) {
						hasMore = reader.next(key, value);
						if (!hasMore) {
							while (moveToNext()) {
								hasMore = reader.next(key, value);
								if (hasMore) {
									//move to the next non-empty split
									break;
								}
							}
						}
						if (!hasMore) {
							return (numBytes == 0) ? -1 : numBytes;
						}

						//check if moved to next block
						blockByteLocation = reader.getPos();
						if(blockByteLocation != NextblockByteLocation)
						{
							//moved to a new block, reset stuff
							//row number
							rowNumber = 1;
							rowNumberBytes = String.valueOf(rowNumber).getBytes("UTF-8");

							//block location
							byteLocation = String.valueOf(NextblockByteLocation).getBytes("UTF-8");
							NextblockByteLocation = blockByteLocation;
						}
						else
						{
							rowNumber += 1;
							rowNumberBytes = String.valueOf(rowNumber).getBytes("UTF-8");
						}

						int sizeOfNextTuple = getTupleSize(value) + 1;
						if (numBytes + sizeOfNextTuple +  rowNumberBytes.length + byteLocation.length + fileName.length + 3 > len) {
							// cannot add tuple to current buffer
							// but the reader has moved pass the fetched tuple
							// we need to store this for a subsequent read call.
							// and return this then.
							pendingValue = true;
							break;
						} else {
							//copy filename
							System.arraycopy(fileName, 0, buffer, offset + numBytes, fileName.length);
							buffer[offset + numBytes + fileName.length] = delimiter;
							numBytes += fileName.length + 1;

							//copy byte location
							System.arraycopy(byteLocation, 0, buffer, offset + numBytes, byteLocation.length);
							buffer[offset + numBytes + byteLocation.length] = delimiter;
							numBytes += byteLocation.length + 1;

							//copy row number
							System.arraycopy(rowNumberBytes, 0, buffer, offset + numBytes, rowNumberBytes.length);
							buffer[offset + numBytes + rowNumberBytes.length] = delimiter;
							numBytes += rowNumberBytes.length + 1;

							copyCurrentTuple(buffer, offset + numBytes);
							buffer[offset + numBytes + sizeOfNextTuple - 1] = (byte) EOL;
							numBytes += sizeOfNextTuple;
						}
					}
					return numBytes;
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

				private RecordReader getRecordReader(int slitIndex) throws IOException {
					RCFileInputFormat format = (RCFileInputFormat) conf.getInputFormat();
					RecordReader reader = format.getRecordReader(
							(org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
					return reader;
				}

			};
		}
		else
		{
			//get content format
			if(configuration.get(KEY_FORMAT).equals(FORMAT_DELIMITED_TEXT))
			{
				LOGGER.info("Creating the indexing input stream with delimiter = "+ configuration.get(KEY_DELIMITER));
				//reading data and RIDs for delimited text
				return new InputStream() {

					private RecordReader<Object, Text> reader;
					private Object key;
					private Text value;
					private boolean hasMore = false;
					private int EOL = "\n".getBytes()[0];
					private Text pendingValue = null;
					private int currentSplitIndex = 0;
					private byte[] fileName;
					private byte[] byteLocation;
					private byte delimiter = ((String)configuration.get(KEY_DELIMITER)).getBytes()[0];
					
					@SuppressWarnings("unchecked")
					private boolean moveToNext() throws IOException {
						for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
							/**
							 * read all the partitions scheduled to the current node
							 */
							if (readSchedule[currentSplitIndex].equals(nodeName)) {
								/**
								 * pick an unread split to read
								 * synchronize among simultaneous partitions in the same machine
								 */
								synchronized (executed) {
									if (executed[currentSplitIndex] == false) {
										executed[currentSplitIndex] = true;
									} else {
										continue;
									}
								}

								/**
								 * read the split
								 */
								reader = getRecordReader(currentSplitIndex);
								key = reader.createKey();
								value = (Text) reader.createValue();
								fileName = ((FileSplit)(inputSplits[currentSplitIndex])).getPath().toUri().getPath().getBytes();
								return true;
							}
						}
						return false;
					}

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {
						if (reader == null) {
							if (!moveToNext()) {
								//nothing to read
								return -1;
							}
						}

						int numBytes = 0;
						if (pendingValue != null) {
							int sizeOfNextTuple = pendingValue.getLength() + 1;
							if (numBytes + sizeOfNextTuple +byteLocation.length + fileName.length + 2> len)
							{
								return numBytes;
							}
							//copy filename
							System.arraycopy(fileName, 0, buffer, offset + numBytes, fileName.length);
							buffer[offset + numBytes + fileName.length] = delimiter;
							numBytes += fileName.length + 1;

							//copy byte location
							System.arraycopy(byteLocation, 0, buffer, offset + numBytes, byteLocation.length);
							buffer[offset + numBytes + byteLocation.length] = delimiter;
							numBytes += byteLocation.length + 1;

							//copy actual value
							System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.getLength());
							buffer[offset + numBytes + pendingValue.getLength()] = (byte) EOL;
							numBytes += pendingValue.getLength() + 1;
							pendingValue = null;
						}

						while (numBytes < len) {
							//get reader position before you actually read
							byteLocation = String.valueOf(reader.getPos()).getBytes();
							hasMore = reader.next(key, value);
							if (!hasMore) {
								while (moveToNext()) {
									//get reader position before you actually read
									byteLocation = String.valueOf(reader.getPos()).getBytes("UTF-8");
									hasMore = reader.next(key, value);
									if (hasMore) {
										//move to the next non-empty split
										break;
									}
								}
							}
							if (!hasMore) {
								return (numBytes == 0) ? -1 : numBytes;
							}
							int sizeOfNextTuple = value.getLength() + 1;
							if (numBytes + sizeOfNextTuple +byteLocation.length + fileName.length + 2> len) {
								// cannot add tuple to current buffer
								// but the reader has moved pass the fetched tuple
								// we need to store this for a subsequent read call.
								// and return this then.
								pendingValue = value;
								break;
							} else {
								//copy filename
								System.arraycopy(fileName, 0, buffer, offset + numBytes, fileName.length);
								buffer[offset + numBytes + fileName.length] = delimiter;
								numBytes += fileName.length + 1;
								
								//copy byte location
								System.arraycopy(byteLocation, 0, buffer, offset + numBytes, byteLocation.length);
								buffer[offset + numBytes + byteLocation.length] = delimiter;
								numBytes += byteLocation.length + 1;

								//Copy actual value
								System.arraycopy(value.getBytes(), 0, buffer, offset + numBytes, value.getLength());
								buffer[offset + numBytes + value.getLength()] = (byte) EOL;
								numBytes += sizeOfNextTuple;
							}
						}
						return numBytes;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}

					private RecordReader getRecordReader(int slitIndex) throws IOException {
						if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
							SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
							RecordReader reader = format.getRecordReader(
									(org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
							return reader;
						} else {
							TextInputFormat format = (TextInputFormat) conf.getInputFormat();
							RecordReader reader = format.getRecordReader(
									(org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
							return reader;
						}
					}

				};
			}
			else if((configuration.get(KEY_FORMAT).equals(FORMAT_ADM)))
			{
				//reading data and RIDs for adm formatted data
				return new InputStream() {

					private RecordReader<Object, Text> reader;
					private Object key;
					private Text value;
					private boolean hasMore = false;
					private int EOL = "\n".getBytes()[0];
					private Text pendingValue = null;
					private int currentSplitIndex = 0;
					private byte[] fileName;
					private byte[] byteLocation;

					@SuppressWarnings("unchecked")
					private boolean moveToNext() throws IOException {
						for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
							/**
							 * read all the partitions scheduled to the current node
							 */
							if (readSchedule[currentSplitIndex].equals(nodeName)) {
								/**
								 * pick an unread split to read
								 * synchronize among simultaneous partitions in the same machine
								 */
								synchronized (executed) {
									if (executed[currentSplitIndex] == false) {
										executed[currentSplitIndex] = true;
									} else {
										continue;
									}
								}

								/**
								 * read the split
								 */
								reader = getRecordReader(currentSplitIndex);
								key = reader.createKey();
								value = (Text) reader.createValue();
								fileName = ((FileSplit)(inputSplits[currentSplitIndex])).getPath().toUri().getPath().getBytes();
								return true;
							}
						}
						return false;
					}

					@Override
					public int read(byte[] buffer, int offset, int len) throws IOException {
						if (reader == null) {
							if (!moveToNext()) {
								//nothing to read
								return -1;
							}
						}

						int numBytes = 0;
						if (pendingValue != null) {
							System.arraycopy(pendingValue.getBytes(), 0, buffer, offset + numBytes, pendingValue.getLength());
							buffer[offset + numBytes + pendingValue.getLength()] = (byte) EOL;
							numBytes += pendingValue.getLength() + 1;
							pendingValue = null;
						}

						while (numBytes < len) {
							//get reader position before you actually read
							byteLocation = String.valueOf(reader.getPos()).getBytes("UTF-8");
							hasMore = reader.next(key, value);
							if (!hasMore) {
								while (moveToNext()) {
									//get reader position before you actually read
									byteLocation = String.valueOf(reader.getPos()).getBytes("UTF-8");
									hasMore = reader.next(key, value);
									if (hasMore) {
										//move to the next non-empty split
										break;
									}
								}
							}
							if (!hasMore) {
								return (numBytes == 0) ? -1 : numBytes;
							}
							//get the index of the first field name
							int firstFieldLocation = value.find("\"");
							int admValueSize = value.getLength();
							if(firstFieldLocation >= 0)
							{
								int sizeOfNextTuple = value.getLength() - firstFieldLocation + 1;
								int sizeOfNextTupleAndRID = fileNameFieldNameWithRecOpeningBraces.length + fileName.length + bytelocationFieldName.length  + byteLocation.length + bytelocationValueEnd.length + sizeOfNextTuple;
								if (numBytes + sizeOfNextTupleAndRID > len) {
									// cannot add tuple to current buffer
									// but the reader has moved pass the fetched tuple
									// we need to store this for a subsequent read call.
									// and return this then.
									pendingValue = value;
									break;
								} else {
									//copy fileNameFieldNameWithRecOpeningBraces
									System.arraycopy(fileNameFieldNameWithRecOpeningBraces, 0, buffer, offset + numBytes,fileNameFieldNameWithRecOpeningBraces.length);
									numBytes += fileNameFieldNameWithRecOpeningBraces.length;
									//copy fileName
									System.arraycopy(fileName, 0, buffer, offset + numBytes,fileName.length);
									numBytes += fileName.length;
									//copy bytelocationFieldName
									System.arraycopy(bytelocationFieldName, 0, buffer, offset + numBytes,bytelocationFieldName.length);
									numBytes += bytelocationFieldName.length;
									//copy byte location value
									System.arraycopy(byteLocation, 0, buffer, offset + numBytes,byteLocation.length);
									numBytes += byteLocation.length;
									//copy byte location field end 
									System.arraycopy(bytelocationValueEnd, 0, buffer, offset + numBytes,bytelocationValueEnd.length);
									numBytes += bytelocationValueEnd.length;
									//copy the actual adm instance
									System.arraycopy(value.getBytes(), firstFieldLocation+1, buffer, offset + numBytes,admValueSize - firstFieldLocation - 1);
									buffer[offset + numBytes + admValueSize - firstFieldLocation] = (byte) EOL;
									numBytes += admValueSize - firstFieldLocation;
								}
							}
						}
						return numBytes;
					}

					@Override
					public int read() throws IOException {
						throw new NotImplementedException("Use read(byte[], int, int");
					}

					private RecordReader getRecordReader(int slitIndex) throws IOException {
						if (conf.getInputFormat() instanceof SequenceFileInputFormat) {
							SequenceFileInputFormat format = (SequenceFileInputFormat) conf.getInputFormat();
							RecordReader reader = format.getRecordReader(
									(org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
							return reader;
						} else {
							TextInputFormat format = (TextInputFormat) conf.getInputFormat();
							RecordReader reader = format.getRecordReader(
									(org.apache.hadoop.mapred.FileSplit) inputSplits[slitIndex], conf, getReporter());
							return reader;
						}
					}

				};
			}
			else
			{
				throw new IOException("Can't index " +configuration.get(KEY_FORMAT)+" input");
			}
		}

	}

	@Override
	public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
		return clusterLocations;
	}
}
