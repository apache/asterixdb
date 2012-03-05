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

import java.io.IOException;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;

/**
 * The connection (session) that serves as the context for communicating with
 * ASTERIX.
 * 
 * @author zheilbron
 */
public interface IAQLJConnection {
    /**
     * Execute an AQL statement that returns an IAQLJResult. The IAQLJResult
     * will contain all associated results of the AQL statement.
     * 
     * @param stmt
     *            the AQL statement
     * @return the results of the AQL statement as an IAQLJResult
     * @throws AQLJException
     */
    public IAQLJResult execute(String stmt) throws AQLJException;

    /**
     * Create a cursor to iterate over results
     * 
     * @return an unpositioned cursor
     */
    public IADMCursor createADMCursor();

    /**
     * Close the connection with the server.
     */
    public void close() throws IOException;
}
