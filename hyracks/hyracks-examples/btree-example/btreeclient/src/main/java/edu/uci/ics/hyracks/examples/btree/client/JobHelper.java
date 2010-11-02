package edu.uci.ics.hyracks.examples.btree.client;

import java.io.File;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class JobHelper {
	public static IFileSplitProvider createFileSplitProvider(String[] splitNCs, String btreeFileName) {
    	FileSplit[] fileSplits = new FileSplit[splitNCs.length];
    	for (int i = 0; i < splitNCs.length; ++i) {
    		String fileName = btreeFileName + "." + splitNCs[i];
            fileSplits[i] = new FileSplit(splitNCs[i], new File(fileName));
        }    	
    	IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);    	
    	return splitProvider;
    }
	
	public static PartitionConstraint createPartitionConstraint(String[] splitNCs) {
    	LocationConstraint[] lConstraints = new LocationConstraint[splitNCs.length];
        for (int i = 0; i < splitNCs.length; ++i) {
            lConstraints[i] = new AbsoluteLocationConstraint(splitNCs[i]);
        }
        return new ExplicitPartitionConstraint(lConstraints);
    }
}
