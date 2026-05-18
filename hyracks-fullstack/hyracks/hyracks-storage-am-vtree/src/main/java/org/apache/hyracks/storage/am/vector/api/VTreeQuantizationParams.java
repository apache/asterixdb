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
package org.apache.hyracks.storage.am.vector.api;

import java.io.Serializable;

/**
 * OptimizedScalarQuantization parameters for a VTree index: the min/max clipping quantiles, the OSQ
 * anisotropic weight ({@code alpha}), the confidence interval, the quantization bit width, and the
 * training sample count. Carried across the Hyracks/asterix layer boundary as named, typed fields.
 */
public record VTreeQuantizationParams(float minQuantile, float maxQuantile, float alpha, float confidenceInterval,
        int bits, int sampleCount) implements Serializable {
    private static final long serialVersionUID = 0L;
}
