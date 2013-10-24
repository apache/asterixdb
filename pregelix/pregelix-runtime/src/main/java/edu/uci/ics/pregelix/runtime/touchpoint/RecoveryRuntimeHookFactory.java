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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHook;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

/**
 * Recover the pregelix job state in a NC
 * 
 * @author yingyib
 */
public class RecoveryRuntimeHookFactory implements IRuntimeHookFactory {
    private static final long serialVersionUID = 1L;
    private final long currentSuperStep;
    private String jobId;
    private IConfigurationFactory confFactory;

    public RecoveryRuntimeHookFactory(String jobId, long currentSuperStep, IConfigurationFactory confFactory) {
        this.currentSuperStep = currentSuperStep;
        this.jobId = jobId;
        this.confFactory = confFactory;
    }

    @Override
    public IRuntimeHook createRuntimeHook() {
        return new IRuntimeHook() {

            @Override
            public void configure(IHyracksTaskContext ctx) throws HyracksDataException {
                IterationUtils.endSuperStep(jobId, ctx);
                Configuration conf = confFactory.createConfiguration(ctx);
                IterationUtils.recoverProperties(jobId, ctx, conf, currentSuperStep);
            }

        };
    }

}
