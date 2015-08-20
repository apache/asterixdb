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
package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.Set;
import java.util.concurrent.Callable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMIOOperation extends Callable<Boolean> {

    public enum LSMIOOpertionType {
        FLUSH,
        MERGE
    }

    public Set<IODeviceHandle> getReadDevices();

    public Set<IODeviceHandle> getWriteDevices();

    public Boolean call() throws HyracksDataException, IndexException;

    public ILSMIOOperationCallback getCallback();

    public String getIndexUniqueIdentifier();

    public LSMIOOpertionType getIOOpertionType();
}
