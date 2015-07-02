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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public interface IIndexOperatorDescriptor extends IActivity {
    public IFileSplitProvider getFileSplitProvider();

    public IStorageManagerInterface getStorageManager();

    public IIndexLifecycleManagerProvider getLifecycleManagerProvider();

    public RecordDescriptor getRecordDescriptor();

    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory();

    public boolean getRetainInput();

    public boolean getRetainNull();

    public INullWriterFactory getNullWriterFactory();

    public ISearchOperationCallbackFactory getSearchOpCallbackFactory();

    public IModificationOperationCallbackFactory getModificationOpCallbackFactory();

    public ITupleFilterFactory getTupleFilterFactory();

    public ILocalResourceFactoryProvider getLocalResourceFactoryProvider();
}
