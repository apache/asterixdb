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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree.LSMRTreeComponent;

public class LSMRTreeComponentFinalizer implements ILSMComponentFinalizer {

	TreeIndexComponentFinalizer treeIndexFinalizer = new TreeIndexComponentFinalizer();
	
	@Override
	public boolean isValid(Object lsmComponent) throws HyracksDataException {
		LSMRTreeComponent component = (LSMRTreeComponent) lsmComponent;
		return treeIndexFinalizer.isValid(component.getRTree())
				&& treeIndexFinalizer.isValid(component.getBTree());
	}

	@Override
	public void finalize(Object lsmComponent) throws HyracksDataException {
		LSMRTreeComponent component = (LSMRTreeComponent) lsmComponent;
		treeIndexFinalizer.finalize(component.getRTree());
		treeIndexFinalizer.finalize(component.getBTree());
	}
}
