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

package org.apache.hyracks.control.nc.io.profiling;

public class IOCounterFactory {

    /**
     * Get the IOCounter for the specific underlying OS
     * 
     * @return an IIOCounter instance
     */
    public IIOCounter getIOCounter() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.indexOf("nix") >= 0 || osName.indexOf("nux") >= 0 || osName.indexOf("aix") >= 0) {
            return new IOCounterLinux();
        } else if (osName.indexOf("mac") >= 0) {
            return new IOCounterOSX();
        } else {
            return new IOCounterDefault();
        }
    }
}
