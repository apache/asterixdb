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
package edu.uci.ics.pregelix.runtime.touchpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.hdfs.ContextFactory;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHook;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;

public class RuntimeHookFactory implements IRuntimeHookFactory {

    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;

    public RuntimeHookFactory(IConfigurationFactory confFactory) {
        this.confFactory = confFactory;
    }

    @Override
    public IRuntimeHook createRuntimeHook() {

        return new IRuntimeHook() {
            private ContextFactory ctxFactory = new ContextFactory();

            @Override
            public void configure(IHyracksTaskContext ctx) throws HyracksDataException {
                Configuration conf = confFactory.createConfiguration(ctx);
                try {
                    TaskAttemptContext mapperContext = ctxFactory.createContext(conf, new TaskAttemptID());
                    mapperContext.getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
                    Vertex.setContext(mapperContext);
                    BspUtils.setDefaultConfiguration(conf);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
