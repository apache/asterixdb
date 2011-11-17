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
package edu.uci.ics.hyracks.api.client;

/**
 * Connection Class used by a Hyracks Client that is colocated in the same VM
 * with the Cluster Controller. Usually, clients must not use this class. This
 * is used internally for testing purposes.
 * 
 * @author vinayakb
 * 
 */
public final class HyracksLocalConnection extends AbstractHyracksConnection {
    public HyracksLocalConnection(IHyracksClientInterface hci) throws Exception {
        super("localhost", hci);
    }
}