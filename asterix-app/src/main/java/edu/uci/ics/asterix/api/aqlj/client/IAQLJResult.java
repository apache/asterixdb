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

/**
 * The results associated with an AQL statement.
 * 
 * @author zheilbron
 */
public interface IAQLJResult extends IADMCursor {
    /**
     * Close the cursor and discard any associated results.
     * It's important to ensure that this method is called in order to free up
     * the associated result buffer.
     */
    public void close() throws IOException;
}
