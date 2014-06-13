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
package edu.uci.ics.pregelix.dataflow.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.VertexContext;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.JobStateUtils;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;
import edu.uci.ics.pregelix.dataflow.context.TaskIterationID;

public class IterationUtils {
    public static final String TMP_DIR = BspUtils.TMP_DIR;

    /**
     * Get the input files' byte size
     *
     * @param job
     */
    public static long getInputFileSize(PregelixJob job) {
        try {
            Path[] paths = FileInputFormat.getInputPaths(job);
            FileSystem dfs = FileSystem.get(job.getConfiguration());
            long size = 0;
            for (Path path : paths) {
                FileStatus fstatus = dfs.getFileStatus(path);
                size += getFileSize(dfs, fstatus.getPath());
            }
            return size;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static long getFileSize(FileSystem dfs, Path path) throws IOException {
        FileStatus fstatus = dfs.getFileStatus(path);
        if (fstatus.isDir()) {
            long totalSize = 0;
            FileStatus[] children = dfs.listStatus(path);
            for (FileStatus child : children) {
                Path childPath = child.getPath();
                totalSize += getFileSize(dfs, childPath);
            }
            return totalSize;
        } else {
            return fstatus.getLen();
        }
    }

    public static void setIterationState(IHyracksTaskContext ctx, String pregelixJobId, int partition, int iteration,
            IStateObject state) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        Map<TaskIterationID, IStateObject> map = context.getAppStateStore(pregelixJobId);
        map.put(new TaskIterationID(pregelixJobId, partition, iteration), state);
    }

    public static IStateObject getIterationState(IHyracksTaskContext ctx, String pregelixJobId, int partition,
            int iteration) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        Map<TaskIterationID, IStateObject> map = context.getAppStateStore(pregelixJobId);
        IStateObject state = map.get(new TaskIterationID(pregelixJobId, partition, iteration));
        return state;
    }

    public static void removeIterationState(IHyracksTaskContext ctx, String pregelixJobId, int partition, int iteration) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        Map<TaskIterationID, IStateObject> map = context.getAppStateStore(pregelixJobId);
        map.remove(new TaskIterationID(pregelixJobId, partition, iteration));
    }

    public static void endSuperStep(String pregelixJobId, IHyracksTaskContext ctx) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.endSuperStep(pregelixJobId);
    }

    public static void setProperties(String pregelixJobId, IHyracksTaskContext ctx, Configuration conf,
            long currentIteration) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.setVertexProperties(pregelixJobId, conf.getLong(PregelixJob.NUM_VERTICE, -1),
                conf.getLong(PregelixJob.NUM_EDGES, -1), currentIteration, ctx.getJobletContext().getClassLoader());
    }

    public static long getSuperstep(String pregelixJobId, IHyracksTaskContext ctx) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        return context.getSuperstep(pregelixJobId);
    }

    public static int getVFrameSize(IHyracksTaskContext ctx) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        return context.getVFrameSize();
    }

    public static void setJobContext(String pregelixJobId, IHyracksTaskContext ctx, TaskAttemptContext tCtx) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.setJobContext(pregelixJobId, tCtx);
    }

    public static VertexContext getVertexContext(String pregelixJobId, IHyracksTaskContext ctx) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        return context.getVertexContext(pregelixJobId);
    }

    public static void recoverProperties(String pregelixJobId, IHyracksTaskContext ctx, Configuration conf,
            long currentIteration) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.recoverVertexProperties(pregelixJobId, conf.getLong(PregelixJob.NUM_VERTICE, -1),
                conf.getLong(PregelixJob.NUM_EDGES, -1), currentIteration, ctx.getJobletContext().getClassLoader());
    }

    public static void writeTerminationState(Configuration conf, String pregelixJobId, boolean terminate)
            throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + pregelixJobId + File.separator + "terminate";
            Path path = new Path(pathStr);
            FSDataOutputStream output = dfs.create(path, true);
            output.writeBoolean(terminate);
            output.flush();
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void writeGlobalAggregateValue(Configuration conf, String pregelixJobId, List<String> aggClassNames,
            List<Writable> aggs) throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + pregelixJobId + File.separator + "global-agg";
            Path path = new Path(pathStr);
            FSDataOutputStream output;
            output = dfs.create(path, true);
            for (int i = 0; i < aggs.size(); i++) {
                //write agg class name
                output.writeUTF(aggClassNames.get(i));
                // write the agg value
                aggs.get(i).write(output);
            }
            output.flush();
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static boolean readTerminationState(Configuration conf, String pregelixJobId) throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + pregelixJobId + File.separator + "terminate";
            Path path = new Path(pathStr);
            FSDataInputStream input = dfs.open(path);
            boolean terminate = input.readBoolean();
            input.close();
            return terminate;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void writeForceTerminationState(Configuration conf, String pregelixJobId) throws HyracksDataException {
        JobStateUtils.writeForceTerminationState(conf, pregelixJobId);
    }

    public static boolean readForceTerminationState(Configuration conf, String jobId) throws HyracksDataException {
        return JobStateUtils.readForceTerminationState(conf, jobId);
    }

    public static Writable readGlobalAggregateValue(Configuration conf, String jobId, String aggClassName)
            throws HyracksDataException {
        return BspUtils.readGlobalAggregateValue(conf, jobId, aggClassName);
    }

    public static HashMap<String, Writable> readAllGlobalAggregateValues(Configuration conf, String jobId)
            throws HyracksDataException {
        return BspUtils.readAllGlobalAggregateValues(conf, jobId);
    }

    public static void makeTempDirectory(Configuration conf) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        String jobId = BspUtils.getJobId(conf);
        String pathStr = TMP_DIR + jobId;
        Path path = new Path(pathStr);
        if (dfs.exists(path)) {
            dfs.mkdirs(path);
        }
    }

    public static void removeTempDirectory(Configuration conf) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        String jobId = BspUtils.getJobId(conf);
        String pathStr = TMP_DIR + jobId;
        Path path = new Path(pathStr);
        if (dfs.exists(path)) {
            dfs.deleteOnExit(path);
        }
    }

}
