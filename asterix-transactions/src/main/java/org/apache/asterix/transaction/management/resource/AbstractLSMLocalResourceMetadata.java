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
package org.apache.asterix.transaction.management.resource;

import org.apache.asterix.common.api.ILocalResourceMetadata;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;

public abstract class AbstractLSMLocalResourceMetadata implements ILocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    protected final int datasetID;
    protected final ITypeTraits[] filterTypeTraits;
    protected final IBinaryComparatorFactory[] filterCmpFactories;
    protected final int[] filterFields;

    public AbstractLSMLocalResourceMetadata(int datasetID, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields) {
        this.datasetID = datasetID;
        this.filterTypeTraits = filterTypeTraits;
        this.filterCmpFactories = filterCmpFactories;
        this.filterFields = filterFields;
    }

    public int getDatasetID() {
        return datasetID;
    }
}
