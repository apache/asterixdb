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

package edu.uci.ics.hyracks.storage.am.common.frames;

import edu.uci.ics.hyracks.storage.am.common.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;

public abstract class AbstractSlotManager implements ISlotManager {
	
	protected final int GREATEST_KEY_INDICATOR = -1;
    protected final int ERROR_INDICATOR = -2;
	
	protected static final int slotSize = 4;
	protected ITreeIndexFrame frame;

	@Override
	public int getTupleOff(int offset) {
		return frame.getBuffer().getInt(offset);
	}

	@Override
	public void setSlot(int offset, int value) {
		frame.getBuffer().putInt(offset, value);
	}

	@Override
	public int getSlotEndOff() {
		return frame.getBuffer().capacity()
				- (frame.getTupleCount() * slotSize);
	}

	@Override
	public int getSlotStartOff() {
		return frame.getBuffer().capacity() - slotSize;
	}

	@Override
	public int getSlotSize() {
		return slotSize;
	}

	@Override
	public void setFrame(ITreeIndexFrame frame) {
		this.frame = frame;
	}

	@Override
	public int getSlotOff(int tupleIndex) {
		return getSlotStartOff() - tupleIndex * slotSize;
	}
	
	@Override
    public int getGreatestKeyIndicator() {
        return GREATEST_KEY_INDICATOR;
    }

    @Override
    public int getErrorIndicator() {
        return ERROR_INDICATOR;
    }
}
