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
package edu.uci.ics.hyracks.storage.common.file;

import java.io.Serializable;

public interface IFileMappingProvider extends Serializable {
    /**
     * Provide the mapping from a file name to an integer id.
     * 
     * @param name
     *            - Name of the file
     * @param create
     *            - Indicate if a new mapping should be created if one does not exist
     * @return The file id on a successful lookup, null if unsuccessful.
     */
    public Integer mapNameToFileId(String name, boolean create);
           
    /**
     * Remove the mapping from a file name to an integer id.
     * 
     * @param name
     *            - Name of the file
     * 
     * @return void
     */
    public void unmapName(String name);
    
    /**
     * Get file id of an already mapped file
     * 
     * @param name
     *            - Name of the file
     * 
     * @return The file id on a successful lookup, null if unsuccessful.
     */
    public Integer getFileId(String name);
}