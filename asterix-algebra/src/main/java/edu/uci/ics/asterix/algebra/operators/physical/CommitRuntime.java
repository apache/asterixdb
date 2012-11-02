package edu.uci.ics.asterix.algebra.operators.physical;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class CommitRuntime implements IPushRuntime {
    
    private RecordDescriptor inputRecordDesc;

    public CommitRuntime(IHyracksTaskContext ctx) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void open() throws HyracksDataException {
        // TODO Auto-generated method stub
        
        //System.out.println("EEEEEEEEEEEEEEEEEEEEE open\n");

    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // TODO Auto-generated method stub

        
        //System.out.println("EEEEEEEEEEEEEEEEEEEEE nextFrame - commit\n");

    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub
        //System.out.println("EEEEEEEEEEEEEEEEEEEEE close\n");
    }

    @Override
    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        throw new IllegalStateException();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
    }

}
