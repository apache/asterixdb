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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.JobStateUtils;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;
import edu.uci.ics.pregelix.dataflow.context.StateKey;

public class IterationUtils {
    public static final String TMP_DIR = "/tmp/";

    public static void setIterationState(IHyracksTaskContext ctx, int partition, IStateObject state) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        Map<StateKey, IStateObject> map = context.getAppStateStore();
        map.put(new StateKey(ctx.getJobletContext().getJobId(), partition), state);
    }

    public static IStateObject getIterationState(IHyracksTaskContext ctx, int partition) {
        JobId currentId = ctx.getJobletContext().getJobId();
        JobId lastId = new JobId(currentId.getId() - 1);
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        Map<StateKey, IStateObject> map = context.getAppStateStore();
        IStateObject state = map.get(new StateKey(lastId, partition));
        while (state == null) {
            /** in case the last job is a checkpointing job */
            lastId = new JobId(lastId.getId() - 1);
            state = map.get(new StateKey(lastId, partition));
        }
        return state;
    }

    public static void removeIterationState(IHyracksTaskContext ctx, int partition) {
        JobId currentId = ctx.getJobletContext().getJobId();
        JobId lastId = new JobId(currentId.getId() - 1);
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        Map<StateKey, IStateObject> map = context.getAppStateStore();
        map.remove(new StateKey(lastId, partition));
    }

    public static void endSuperStep(String giraphJobId, IHyracksTaskContext ctx) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.endSuperStep(giraphJobId);
    }

    public static void setProperties(String jobId, IHyracksTaskContext ctx, Configuration conf, long currentIteration) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.setVertexProperties(jobId, conf.getLong(PregelixJob.NUM_VERTICE, -1),
                conf.getLong(PregelixJob.NUM_EDGES, -1), currentIteration);
    }

    public static void recoverProperties(String jobId, IHyracksTaskContext ctx, Configuration conf,
            long currentIteration) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
        context.recoverVertexProperties(jobId, conf.getLong(PregelixJob.NUM_VERTICE, -1),
                conf.getLong(PregelixJob.NUM_EDGES, -1), currentIteration);
    }

    public static void writeTerminationState(Configuration conf, String jobId, boolean terminate)
            throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + jobId;
            Path path = new Path(pathStr);
            FSDataOutputStream output = dfs.create(path, true);
            output.writeBoolean(terminate);
            output.flush();
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void writeGlobalAggregateValue(Configuration conf, String jobId, Writable agg)
            throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + jobId + "agg";
            Path path = new Path(pathStr);
            FSDataOutputStream output = dfs.create(path, true);
            agg.write(output);
            output.flush();
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static boolean readTerminationState(Configuration conf, String jobId) throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + jobId;
            Path path = new Path(pathStr);
            FSDataInputStream input = dfs.open(path);
            boolean terminate = input.readBoolean();
            input.close();
            return terminate;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static void writeForceTerminationState(Configuration conf, String jobId) throws HyracksDataException {
        JobStateUtils.writeForceTerminationState(conf, jobId);
    }

    public static boolean readForceTerminationState(Configuration conf, String jobId) throws HyracksDataException {
        return JobStateUtils.readForceTerminationState(conf, jobId);
    }

    public static Writable readGlobalAggregateValue(Configuration conf, String jobId) throws HyracksDataException {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + jobId + "agg";
            Path path = new Path(pathStr);
            FSDataInputStream input = dfs.open(path);
            Writable agg = BspUtils.createFinalAggregateValue(conf);
            agg.readFields(input);
            input.close();
            return agg;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
