/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.job;

import java.io.IOException;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSpecificationFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

public class DeserializingJobSpecificationFactory implements IJobSpecificationFactory {
    public static final IJobSpecificationFactory INSTANCE = new DeserializingJobSpecificationFactory();

    private DeserializingJobSpecificationFactory() {
    }

    @Override
    public JobSpecification createJobSpecification(byte[] bytes, ICCBootstrap bootstrap, ICCApplicationContext appCtx)
            throws HyracksException {
        try {
            return (JobSpecification) JavaSerializationUtils.deserialize(bytes, appCtx.getClassLoader());
        } catch (IOException e) {
            throw new HyracksException(e);
        } catch (ClassNotFoundException e) {
            throw new HyracksException(e);
        }
    }
}