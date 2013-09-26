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
package edu.uci.ics.pregelix.dataflow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.hdfs.ContextFactory;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;

public class VertexFileWriteOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;
    private final IRecordDescriptorFactory inputRdFactory;

    public VertexFileWriteOperatorDescriptor(JobSpecification spec, IConfigurationFactory confFactory,
            IRecordDescriptorFactory inputRdFactory) {
        super(spec, 1, 0);
        this.confFactory = confFactory;
        this.inputRdFactory = inputRdFactory;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private RecordDescriptor rd0;
            private FrameDeserializer frameDeserializer;
            private Configuration conf;
            private VertexWriter vertexWriter;
            private TaskAttemptContext context;
            private String TEMP_DIR = "_temporary";
            private ClassLoader ctxCL;
            private ContextFactory ctxFactory = new ContextFactory();

            @Override
            public void open() throws HyracksDataException {
                rd0 = inputRdFactory == null ? recordDescProvider.getInputRecordDescriptor(getActivityId(), 0)
                        : inputRdFactory.createRecordDescriptor(ctx);
                frameDeserializer = new FrameDeserializer(ctx.getFrameSize(), rd0);
                ctxCL = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                conf = confFactory.createConfiguration(ctx);

                VertexOutputFormat outputFormat = BspUtils.createVertexOutputFormat(conf);
                context = ctxFactory.createContext(conf, partition);
                context.getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
                try {
                    vertexWriter = outputFormat.createVertexWriter(context);
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                frameDeserializer.reset(frame);
                try {
                    while (!frameDeserializer.done()) {
                        Object[] tuple = frameDeserializer.deserializeRecord();
                        Vertex value = (Vertex) tuple[1];
                        vertexWriter.writeVertex(value);
                    }
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                Thread.currentThread().setContextClassLoader(ctxCL);
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    vertexWriter.close(context);
                    moveFilesToFinalPath();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            private void moveFilesToFinalPath() throws HyracksDataException {
                try {
                    JobContext job = ctxFactory.createJobContext(conf);
                    Path outputPath = FileOutputFormat.getOutputPath(job);
                    FileSystem dfs = FileSystem.get(conf);
                    Path filePath = new Path(outputPath, "part-" + new Integer(partition).toString());
                    FileStatus[] results = findPartitionPaths(outputPath, dfs);
                    if (results.length >= 1) {
                        /**
                         * for Hadoop-0.20.2
                         */
                        renameFile(dfs, filePath, results);
                    } else {
                        /**
                         * for Hadoop-0.23.1
                         */
                        int jobId = job.getJobID().getId();
                        outputPath = new Path(outputPath.toString() + File.separator + TEMP_DIR + File.separator
                                + jobId);
                        results = findPartitionPaths(outputPath, dfs);
                        renameFile(dfs, filePath, results);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                } finally {
                    Thread.currentThread().setContextClassLoader(ctxCL);
                }
            }

            private FileStatus[] findPartitionPaths(Path outputPath, FileSystem dfs) throws FileNotFoundException,
                    IOException {
                FileStatus[] tempPaths = dfs.listStatus(outputPath, new PathFilter() {
                    @Override
                    public boolean accept(Path dir) {
                        return dir.getName().endsWith(TEMP_DIR) && dir.getName().indexOf(".crc") < 0;
                    }
                });
                Path tempDir = tempPaths[0].getPath();
                FileStatus[] results = dfs.listStatus(tempDir, new PathFilter() {
                    @Override
                    public boolean accept(Path dir) {
                        return dir.getName().indexOf(context.getTaskAttemptID().toString()) >= 0
                                && dir.getName().indexOf(".crc") < 0;
                    }
                });
                return results;
            }

            private void renameFile(FileSystem dfs, Path filePath, FileStatus[] results) throws IOException,
                    HyracksDataException, FileNotFoundException {
                Path srcDir = results[0].getPath();
                if (!dfs.exists(srcDir))
                    throw new HyracksDataException("file " + srcDir.toString() + " does not exist!");

                FileStatus[] srcFiles = dfs.listStatus(srcDir);
                Path srcFile = srcFiles[0].getPath();
                dfs.delete(filePath, true);
                dfs.rename(srcFile, filePath);
            }

        };
    }
}
