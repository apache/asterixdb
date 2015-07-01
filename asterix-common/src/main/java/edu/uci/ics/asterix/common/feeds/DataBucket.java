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
package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class DataBucket {

    private static final AtomicInteger globalBucketId = new AtomicInteger(0);

    private final ByteBuffer content;
    private final AtomicInteger readCount;
    private final int bucketId;

    private int desiredReadCount;
    private ContentType contentType;

    private final DataBucketPool pool;

    public enum ContentType {
        DATA, // data (feed tuple)
        EOD, // A signal indicating that there shall be no more data
        EOSD // End of processing of spilled data
    }

    public DataBucket(DataBucketPool pool) {
        this.content = ByteBuffer.allocate(pool.getFrameSize());
        this.readCount = new AtomicInteger(0);
        this.pool = pool;
        this.contentType = ContentType.DATA;
        this.bucketId = globalBucketId.incrementAndGet();
    }

    public synchronized void reset(ByteBuffer frame) {
        if (frame != null) {
            content.flip();
            System.arraycopy(frame.array(), 0, content.array(), 0, frame.limit());
            content.limit(frame.limit());
            content.position(0);
        }
    }

    public synchronized void doneReading() {
        if (readCount.incrementAndGet() == desiredReadCount) {
            readCount.set(0);
            pool.returnDataBucket(this);
        }
    }

    public void setDesiredReadCount(int rCount) {
        this.desiredReadCount = rCount;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public void setContentType(ContentType contentType) {
        this.contentType = contentType;
    }

    public synchronized ByteBuffer getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "DataBucket [" + bucketId + "]" + " (" + readCount + "," + desiredReadCount + ")";
    }

}