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
package edu.uci.ics.hyracks.storage.am.common.api;

import java.nio.ByteBuffer;

public interface ISplitKey {
	public void initData(int keySize);

	public void reset();

	public ByteBuffer getBuffer();

	public ITreeIndexTupleReference getTuple();

	public int getLeftPage();

	public int getRightPage();

	public void setLeftPage(int leftPage);

	public void setRightPage(int rightPage);

	public void setPages(int leftPage, int rightPage);

	public ISplitKey duplicate(ITreeIndexTupleReference copyTuple);
}
