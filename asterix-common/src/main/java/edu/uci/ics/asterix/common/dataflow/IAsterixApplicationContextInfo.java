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
package edu.uci.ics.asterix.common.dataflow;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

/**
 * Provides methods for obtaining the IIndexLifecycleManagerProvider, IStorageManagerInterface and
 * ICCApplicationContext implementation.
 */
public interface IAsterixApplicationContextInfo {

    /**
     * Returns an instance of the implementation for IIndexLifecycleManagerProvider.
     * 
     * @return IIndexLifecycleManagerProvider implementation instance
     */
    public IIndexLifecycleManagerProvider getIndexLifecycleManagerProvider();

    /**
     * Returns an instance of the implementation for IStorageManagerInterface.
     * 
     * @return IStorageManagerInterface implementation instance
     */
    public IStorageManagerInterface getStorageManagerInterface();

    /**
     * Returns an instance of the implementation for ICCApplicationContext.
     * 
     * @return ICCApplicationContext implementation instance
     */
    public ICCApplicationContext getCCApplicationContext();
}
