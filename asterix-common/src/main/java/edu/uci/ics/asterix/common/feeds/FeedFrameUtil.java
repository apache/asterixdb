package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class FeedFrameUtil {

    public static ByteBuffer getSlicedFrame(IHyracksTaskContext ctx, int tupleIndex, FrameTupleAccessor fta) throws HyracksDataException {
        FrameTupleAppender appender = new FrameTupleAppender();
        IFrame slicedFrame = new VSizeFrame(ctx);
        appender.reset(slicedFrame, true);
        int startTupleIndex = tupleIndex + 1;
        int totalTuples = fta.getTupleCount();
        for (int ti = startTupleIndex; ti < totalTuples; ti++) {
            appender.append(fta, ti);
        }
        return slicedFrame.getBuffer();
    }

    public static ByteBuffer getSampledFrame(IHyracksTaskContext ctx, FrameTupleAccessor fta, int sampleSize) throws HyracksDataException {
        NChooseKIterator it = new NChooseKIterator(fta.getTupleCount(), sampleSize);
        FrameTupleAppender appender = new FrameTupleAppender();
        IFrame sampledFrame = new VSizeFrame(ctx);
        appender.reset(sampledFrame, true);
        int nextTupleIndex = 0;
        while (it.hasNext()) {
            nextTupleIndex = it.next();
            appender.append(fta, nextTupleIndex);
        }
        return sampledFrame.getBuffer();
    }
    
  

    private static class NChooseKIterator {

        private final int n;
        private final int k;
        private final BitSet bSet;
        private final Random random;

        private int traversed = 0;

        public NChooseKIterator(int n, int k) {
            this.n = n;
            this.k = k;
            this.bSet = new BitSet(n);
            bSet.set(0, n - 1);
            this.random = new Random();
        }

        public boolean hasNext() {
            return traversed < k;
        }

        public int next() {
            if (hasNext()) {
                traversed++;
                int startOffset = random.nextInt(n);
                int pos = -1;
                while (pos < 0) {
                    pos = bSet.nextSetBit(startOffset);
                    if (pos < 0) {
                        startOffset = 0;
                    }
                }
                bSet.clear(pos);
                return pos;
            } else {
                return -1;
            }
        }

    }

}
