package edu.uci.ics.pregelix.dataflow.context;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.pregelix.api.graph.VertexContext;

public class PJobContext {
    private static final Logger LOGGER = Logger.getLogger(RuntimeContext.class.getName());

    private final Map<Long, List<FileReference>> iterationToFiles = new ConcurrentHashMap<Long, List<FileReference>>();
    private final Map<TaskIterationID, IStateObject> appStateMap = new ConcurrentHashMap<TaskIterationID, IStateObject>();
    private Long jobIdToSuperStep;
    private Boolean jobIdToMove;
    private VertexContext vCtx = new VertexContext();

    public void close() throws HyracksDataException {
        for (Entry<Long, List<FileReference>> entry : iterationToFiles.entrySet())
            for (FileReference fileRef : entry.getValue())
                fileRef.delete();

        iterationToFiles.clear();
        appStateMap.clear();
    }

    public void clearState() throws HyracksDataException {
        for (Entry<Long, List<FileReference>> entry : iterationToFiles.entrySet())
            for (FileReference fileRef : entry.getValue()) {
                if (fileRef != null) {
                    fileRef.delete();
                }
            }

        iterationToFiles.clear();
        appStateMap.clear();
    }

    public Map<TaskIterationID, IStateObject> getAppStateStore() {
        return appStateMap;
    }

    public static RuntimeContext get(IHyracksTaskContext ctx) {
        return (RuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject();
    }

    public void setVertexProperties(long numVertices, long numEdges, long currentIteration, ClassLoader cl) {
        if (jobIdToMove == null || jobIdToMove == true) {
            if (jobIdToSuperStep == null) {
                if (currentIteration <= 0) {
                    jobIdToSuperStep = 0L;
                } else {
                    jobIdToSuperStep = currentIteration;
                }
            }

            long superStep = jobIdToSuperStep;
            List<FileReference> files = iterationToFiles.remove(superStep - 1);
            if (files != null) {
                for (FileReference fileRef : files) {
                    if (fileRef != null) {
                        fileRef.delete();
                    }
                }
            }

            setProperties(numVertices, numEdges, currentIteration, superStep, false, cl);
        }
    }

    public void recoverVertexProperties(long numVertices, long numEdges, long currentIteration, ClassLoader cl) {
        if (jobIdToSuperStep == null) {
            if (currentIteration <= 0) {
                jobIdToSuperStep = 0L;
            } else {
                jobIdToSuperStep = currentIteration;
            }
        }

        long superStep = jobIdToSuperStep;
        List<FileReference> files = iterationToFiles.remove(superStep - 1);
        if (files != null) {
            for (FileReference fileRef : files) {
                if (fileRef != null) {
                    fileRef.delete();
                }
            }
        }

        setProperties(numVertices, numEdges, currentIteration, superStep, true, cl);
    }

    public void endSuperStep() {
        jobIdToMove = true;
        LOGGER.info("end iteration " + vCtx.getSuperstep());
    }

    public Map<Long, List<FileReference>> getIterationToFiles() {
        return iterationToFiles;
    }

    public VertexContext getVertexContext() {
        return vCtx;
    }

    private void setProperties(long numVertices, long numEdges, long currentIteration, long superStep, boolean toMove,
            ClassLoader cl) {
        try {
            if (currentIteration > 0) {
                vCtx.setSuperstep(currentIteration);
            } else {
                vCtx.setSuperstep(++superStep);
            }
            vCtx.setNumVertices(numVertices);
            vCtx.setNumEdges(numEdges);

            jobIdToSuperStep = superStep;
            jobIdToMove = toMove;
            LOGGER.info("start iteration " + vCtx.getSuperstep());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
