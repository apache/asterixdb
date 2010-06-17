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
package edu.uci.ics.hyracks.coreops.util;

import java.util.Collection;

public class SynchronizedBoundedBuffer<T> {
    private static final int QUEUE_SIZE = 8192;
    private Object[] buffer;
    private int head;
    private int tail;

    public SynchronizedBoundedBuffer() {
        buffer = new Object[QUEUE_SIZE];
        head = 0;
        tail = 0;
    }

    public synchronized void put(T o) throws InterruptedException {
        while (full()) {
            wait();
        }
        buffer[tail] = o;
        tail = (tail + 1) % QUEUE_SIZE;
        notifyAll();
    }

    public synchronized void putAll(Collection<? extends T> c) throws InterruptedException {
        for (T o : c) {
            while (full()) {
                wait();
            }
            buffer[tail] = o;
            tail = (tail + 1) % QUEUE_SIZE;
        }
        notifyAll();
    }

    public synchronized T get() throws InterruptedException {
        while (empty()) {
            wait();
        }
        T o = (T) buffer[head];
        head = (head + 1) % QUEUE_SIZE;
        notifyAll();
        return o;
    }

    private boolean empty() {
        return head == tail;
    }

    private boolean full() {
        return (tail + 1) % QUEUE_SIZE == head;
    }
}