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
package org.apache.asterix.external.indexing.dataflow;

import java.util.Map;

import org.apache.asterix.external.adapter.factory.IControlledAdapterFactory;
import org.apache.asterix.external.dataset.adapter.IControlledAdapter;
import org.apache.asterix.external.indexing.ExternalFileIndexAccessor;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

// This class takes care of creating the adapter based on the formats and input format
public class HDFSLookupAdapterFactory implements IControlledAdapterFactory {

    private static final long serialVersionUID = 1L;

    private Map<String, String> adapterConfiguration;
    private IAType atype;
    private boolean propagateInput;
    private int[] ridFields;
    private int[] propagatedFields;
    private boolean retainNull;

    @Override
    public void configure(IAType atype, boolean propagateInput, int[] ridFields,
            Map<String, String> adapterConfiguration, boolean retainNull) {
        this.adapterConfiguration = adapterConfiguration;
        this.atype = atype;
        this.propagateInput = propagateInput;
        this.ridFields = ridFields;
        this.retainNull = retainNull;
    }

    @Override
    public IControlledAdapter createAdapter(IHyracksTaskContext ctx, ExternalFileIndexAccessor fileIndexAccessor,
            RecordDescriptor inRecDesc) {
        if (propagateInput) {
            configurePropagatedFields(inRecDesc);
        }
        return new HDFSLookupAdapter(atype, inRecDesc, adapterConfiguration, propagateInput, ridFields,
                propagatedFields, ctx, fileIndexAccessor, retainNull);
    }

    private void configurePropagatedFields(RecordDescriptor inRecDesc) {
        int ptr = 0;
        boolean skip = false;
        propagatedFields = new int[inRecDesc.getFieldCount() - ridFields.length];
        for (int i = 0; i < inRecDesc.getFieldCount(); i++) {
            if (ptr < ridFields.length) {
                skip = false;
                for (int j = 0; j < ridFields.length; j++) {
                    if (ridFields[j] == i) {
                        ptr++;
                        skip = true;
                        break;
                    }
                }
                if (!skip)
                    propagatedFields[i - ptr] = i;
            } else {
                propagatedFields[i - ptr] = i;
            }
        }
    }
}
