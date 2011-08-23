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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IndexRegistry<IndexType> {

	private HashMap<Integer, IndexType> map = new HashMap<Integer, IndexType>();
	private Lock registryLock = new ReentrantLock();

	public IndexType get(int fileId) {
		return map.get(fileId);
	}

	public void lock() {
		registryLock.lock();
	}

	public void unlock() {
		registryLock.unlock();
	}

	public void register(int fileId, IndexType index) {
		map.put(fileId, index);
	}

	public void unregister(int fileId) {
		try {
			map.remove(fileId);
		} catch (Exception e) {
		}
	}

	public int size() {
		return map.size();
	}
}
