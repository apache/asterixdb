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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.io.IOException;

/**
 * Represent a buffer that is backed by a physical file. Provides custom APIs
 * for accessing a chunk of the underlying file.
 */

public interface IFileBasedBuffer extends IBuffer {

    public void flush() throws IOException;

    /**
     * Resets the buffer with content (size as specified) from a given file
     * starting at offset.
     */
    public void reset(String filePath, long offset, int size) throws IOException;

    public long getNextWritePosition();

    public void setNextWritePosition(long writePosition);

    public void close() throws IOException;
    
    public void open(String filePath, long offset, int size) throws IOException;

}
