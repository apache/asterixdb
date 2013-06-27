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
package edu.uci.ics.asterix.installer.service;

import java.util.List;

import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public interface ILookupService {

    public void writeAsterixInstance(AsterixInstance asterixInstance) throws Exception;

    public AsterixInstance getAsterixInstance(String name) throws Exception;

    public boolean isRunning(Configuration conf) throws Exception;

    public void startService(Configuration conf) throws Exception;

    public void stopService(Configuration conf) throws Exception;

    public boolean exists(String name) throws Exception;

    public void removeAsterixInstance(String name) throws Exception;

    public List<AsterixInstance> getAsterixInstances() throws Exception;

    public void updateAsterixInstance(AsterixInstance updatedInstance) throws Exception;
}
