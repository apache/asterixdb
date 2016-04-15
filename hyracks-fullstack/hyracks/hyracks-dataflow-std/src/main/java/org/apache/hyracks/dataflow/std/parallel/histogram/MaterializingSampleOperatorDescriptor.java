/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.dataflow.std.parallel.histogram;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.parallel.HistogramAlgorithm;

/**
 * @author michael
 */
public class MaterializingSampleOperatorDescriptor extends AbstractSampleOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    /**
     * @param spec
     * @param frameLimit
     * @param sampleFields
     * @param sampleBasis
     * @param rDesc
     * @param compFactories
     * @param alg
     * @param outputArity
     */
    public MaterializingSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int[] sampleFields,
            int sampleBasis, RecordDescriptor rDesc, IBinaryComparatorFactory[] compFactories, HistogramAlgorithm alg,
            int outputArity) {
        super(spec, frameLimit, sampleFields, sampleBasis, rDesc, compFactories, alg, outputArity);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param spec
     * @param frameLimit
     * @param sampleFields
     * @param sampleBasis
     * @param rDesc
     * @param compFactories
     * @param alg
     * @param outputArity
     * @param outputMaterializationFlags
     */
    public MaterializingSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int[] sampleFields,
            int sampleBasis, RecordDescriptor rDesc, IBinaryComparatorFactory[] compFactories, HistogramAlgorithm alg,
            int outputArity, boolean[] outputMaterializationFlags) {
        super(spec, frameLimit, sampleFields, sampleBasis, rDesc, compFactories, alg, outputArity,
                outputMaterializationFlags);
        // TODO Auto-generated constructor stub
    }

}
