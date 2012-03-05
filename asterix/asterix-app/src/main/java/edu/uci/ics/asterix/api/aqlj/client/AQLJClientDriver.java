/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.client;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;

/**
 * This class encapsulates the mechanism for creating a connection to an ASTERIX
 * server.
 * 
 * @author zheilbron
 */
public class AQLJClientDriver {
    /**
     * Get a connection to the ASTERIX server.
     * 
     * @param host
     *            the ip or hostname of the ASTERIX server
     * @param port
     *            the port of the ASTERIX server (default: 14600)
     * @param dataverse
     *            the name of the dataverse to use for any AQL statements
     * @return an IAQLJConnection object representing the connection to ASTERIX
     * @throws AQLJException
     */
    public static IAQLJConnection getConnection(String host, int port, String dataverse) throws AQLJException {
        return new AQLJConnection(host, port, dataverse);
    }
}
