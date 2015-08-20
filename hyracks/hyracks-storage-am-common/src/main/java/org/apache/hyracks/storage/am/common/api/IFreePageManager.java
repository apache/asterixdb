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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IFreePageManager {
	public void open(int fileId);
	
	public void close();
	
	public int getFreePage(ITreeIndexMetaDataFrame metaFrame)
			throws HyracksDataException;

	public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage)
			throws HyracksDataException;

	public int getMaxPage(ITreeIndexMetaDataFrame metaFrame)
			throws HyracksDataException;

	public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage)
			throws HyracksDataException;

	public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory();

	// required to return negative values
	public byte getMetaPageLevelIndicator();

	public byte getFreePageLevelIndicator();

	// determined by examining level indicator
	public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame);

	public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame);
	
	public int getFirstMetadataPage();		
}
