/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class NCFileSystemAdapter extends FileSystemBasedAdapter {

	private static final long serialVersionUID = -4154256369973615710L;
	private FileSplit[] fileSplits;

	public static final String KEY_SPLITS = "path";
	
	public NCFileSystemAdapter(IAType atype) {
		super(atype);
	}

	@Override
	public void configure(Map<String, String> arguments) throws Exception {
		this.configuration = arguments;
		String[] splits = arguments.get(KEY_SPLITS).split(",");
		configureFileSplits(splits);
		configurePartitionConstraint();
		configureFormat();
	}

	@Override
	public void initialize(IHyracksTaskContext ctx) throws Exception {
		this.ctx = ctx;
	}

	@Override
	public AdapterType getAdapterType() {
		return AdapterType.READ;
	}

	private void configureFileSplits(String[] splits) {
		if (fileSplits == null) {
			fileSplits = new FileSplit[splits.length];
			String nodeName;
			String nodeLocalPath;
			int count = 0;
			for (String splitPath : splits) {
				nodeName = splitPath.split(":")[0];
				nodeLocalPath = splitPath.split("://")[1];
				FileSplit fileSplit = new FileSplit(nodeName,
						new FileReference(new File(nodeLocalPath)));
				fileSplits[count++] = fileSplit;
			}
		}
	}

	private void configurePartitionConstraint() {
		String[] locs = new String[fileSplits.length];
		for (int i = 0; i < fileSplits.length; i++) {
			locs[i] = fileSplits[i].getNodeName();
		}
		partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locs);
	}

	@Override
	public InputStream getInputStream(int partition) throws IOException {
		FileSplit split = fileSplits[partition];
		File inputFile = split.getLocalFile().getFile();
		InputStream in;
		try {
			in = new FileInputStream(inputFile);
			return in;
		} catch (FileNotFoundException e) {
			throw new IOException(e);
		}
	}

}
