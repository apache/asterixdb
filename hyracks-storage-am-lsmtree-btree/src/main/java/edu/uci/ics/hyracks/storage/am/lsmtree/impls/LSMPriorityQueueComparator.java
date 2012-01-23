package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import java.util.Comparator;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class LSMPriorityQueueComparator implements Comparator<LSMPriorityQueueElement> {

	private MultiComparator cmp;
	
	public LSMPriorityQueueComparator(MultiComparator cmp) {
		this.cmp = cmp;
	}
	
	@Override
	public int compare(LSMPriorityQueueElement elementA, LSMPriorityQueueElement elementB) {
		
		int result = cmp.compare(elementA.getTuple(), elementB.getTuple());
		
		if(result == 1) {
			return 1;
		}
		else if(result == -1) {
			return -1;
		}
		else {
			if(elementA.getCursorIndex() > elementB.getCursorIndex()) {
				return 1;
			}
			else {
				return -1;
			}
		}
	}

}
