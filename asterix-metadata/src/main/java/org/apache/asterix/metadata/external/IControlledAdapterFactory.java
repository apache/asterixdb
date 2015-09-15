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
package org.apache.asterix.metadata.external;

import java.io.Serializable;
import java.util.Map;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public interface IControlledAdapterFactory extends Serializable {
    public IControlledAdapter createAdapter(IHyracksTaskContext ctx, ExternalFileIndexAccessor fileIndexAccessor,
            RecordDescriptor inRecDesc);

    public void configure(IAType atype, boolean propagateInput, int[] ridFields,
            Map<String, String> adapterConfiguration, boolean retainNull);
}
