package edu.uci.ics.hyracks.dataflow.std.sort;

import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;

/**
 * @author pouria
 * 
 *         Interface for the Run Generator
 */
public interface IRunGenerator extends IFrameWriter {

    /**
     * @return the list of generated (sorted) runs
     */
    public List<IFrameReader> getRuns();
}