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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public interface IIndexOperatorDescriptor extends IActivity {
    public IFileSplitProvider getFileSplitProvider();

    public IStorageManagerInterface getStorageManager();

    public IIndexLifecycleManagerProvider getLifecycleManagerProvider();

    public RecordDescriptor getRecordDescriptor();

    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory();

    public boolean getRetainInput();

    public ISearchOperationCallbackFactory getSearchOpCallbackFactory();
    
    public IModificationOperationCallbackFactory getModificationOpCallbackFactory();
    
    public ITupleFilterFactory getTupleFilterFactory();
    
    public ILocalResourceFactoryProvider getLocalResourceFactoryProvider();
}
