package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class Partition implements Serializable {

	private String uri;
	private long offset;
	private long length;
	private String[] locations;

	public Partition() {
	}

	public Partition(FileSplit file) {
		uri = file.getPath().toUri().toString();
		offset = file.getStart();
		length = file.getLength();
		try {
			locations = file.getLocations();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public FileSplit toFileSplit() {
		return new FileSplit(new Path(uri), offset, length, locations);
	}
}
