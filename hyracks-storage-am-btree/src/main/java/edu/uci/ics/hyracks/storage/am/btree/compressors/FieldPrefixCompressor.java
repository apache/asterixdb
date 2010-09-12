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

package edu.uci.ics.hyracks.storage.am.btree.compressors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IFrameCompressor;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;

public class FieldPrefixCompressor implements IFrameCompressor {
	
	// minimum ratio of uncompressed records to total records to consider re-compression
	private float ratioThreshold;
	
	// minimum number of records matching field prefixes to consider compressing them 
	private int occurrenceThreshold;
	
	public FieldPrefixCompressor(float ratioThreshold, int occurrenceThreshold) {
		this.ratioThreshold = ratioThreshold;
		this.occurrenceThreshold = occurrenceThreshold;
	}
	
	@Override
	public boolean compress(FieldPrefixNSMLeafFrame frame, MultiComparator cmp) throws Exception {		
		int numRecords = frame.getNumRecords();
    	if(numRecords <= 0) {
            frame.setNumPrefixRecords(0);
            frame.setFreeSpaceOff(frame.getOrigFreeSpaceOff());
            frame.setTotalFreeSpace(frame.getOrigTotalFreeSpace());            
            return false;
        }
    	    	
    	int numUncompressedRecords = frame.getNumUncompressedRecords();
    	float ratio = (float)numUncompressedRecords / (float)numRecords;    	
    	if(ratio < ratioThreshold) return false;    	
    	
        IBinaryComparator[] cmps = cmp.getComparators();
        IFieldAccessor[] fields = cmp.getFields();
        
        ByteBuffer buf = frame.getBuffer();
        byte[] pageArray = buf.array();
        IPrefixSlotManager slotManager = frame.slotManager;
                
        // perform analysis pass
        ArrayList<KeyPartition> keyPartitions = getKeyPartitions(frame, cmp, occurrenceThreshold);
        if(keyPartitions.size() == 0) return false;        
        
        // for each keyPartition, determine the best prefix length for compression, and count how many prefix records we would need in total
        int totalSlotsNeeded = 0;
        int totalPrefixBytes = 0;
        for(KeyPartition kp : keyPartitions) {        	
        	         	
        	for(int j = 0; j < kp.pmi.length; j++) {
                int benefitMinusCost = kp.pmi[j].spaceBenefit - kp.pmi[j].spaceCost;            
                if(benefitMinusCost > kp.maxBenefitMinusCost) {
                    kp.maxBenefitMinusCost = benefitMinusCost;
                    kp.maxPmiIndex = j;
                }
            }
        	        	
        	// ignore keyPartitions with no benefit and don't count bytes and slots needed
        	if(kp.maxBenefitMinusCost <= 0) continue; 
            
            totalPrefixBytes += kp.pmi[kp.maxPmiIndex].prefixBytes;
            totalSlotsNeeded += kp.pmi[kp.maxPmiIndex].prefixSlotsNeeded;
        }
        
        //System.out.println("TOTAL SLOTS NEEDED: " + totalSlotsNeeded);
        
        // we use a greedy heuristic to solve this "knapsack"-like problem
        // (every keyPartition has a space savings and a number of slots required, but we the number of slots are constrained by MAX_PREFIX_SLOTS)
        // we sort the keyPartitions by maxBenefitMinusCost / prefixSlotsNeeded and later choose the top MAX_PREFIX_SLOTS        
        int[] newPrefixSlots;
        if(totalSlotsNeeded > FieldPrefixSlotManager.MAX_PREFIX_SLOTS) {        	        	
        	// order keyPartitions by the heuristic function
            SortByHeuristic heuristicComparator = new SortByHeuristic();
            Collections.sort(keyPartitions, heuristicComparator);
            int slotsUsed = 0;
            int numberKeyPartitions = -1;
            for(int i = 0; i < keyPartitions.size(); i++) {
                KeyPartition kp = keyPartitions.get(i); 
                slotsUsed += kp.pmi[kp.maxPmiIndex].prefixSlotsNeeded;
                if(slotsUsed > FieldPrefixSlotManager.MAX_PREFIX_SLOTS) {
                    numberKeyPartitions = i + 1;
                    slotsUsed -= kp.pmi[kp.maxPmiIndex].prefixSlotsNeeded;
                    break;
                }                
            }
            newPrefixSlots = new int[slotsUsed];
            
            // remove irrelevant keyPartitions and adjust total prefix bytes
            while(keyPartitions.size() >= numberKeyPartitions) {            	            	
            	int lastIndex = keyPartitions.size() - 1;
            	KeyPartition kp = keyPartitions.get(lastIndex);
            	if(kp.maxBenefitMinusCost > 0) totalPrefixBytes -= kp.pmi[kp.maxPmiIndex].prefixBytes;            	
            	keyPartitions.remove(lastIndex);
            }
            
            // re-order keyPartitions by prefix (corresponding to original order)
            SortByOriginalRank originalRankComparator = new SortByOriginalRank();
            Collections.sort(keyPartitions, originalRankComparator);                        
        }
        else {
            newPrefixSlots = new int[totalSlotsNeeded];
        }
        
        //System.out.println("TOTALPREFIXBYTES: " + totalPrefixBytes);
        
        int[] newRecordSlots = new int[numRecords];
        
        // WARNING: our hope is that compression is infrequent
        // here we allocate a big chunk of memory to temporary hold the new, re-compressed records
        // in general it is very hard to avoid this step        
        int prefixFreeSpace = frame.getOrigFreeSpaceOff();
        int recordFreeSpace = prefixFreeSpace + totalPrefixBytes;
        byte[] buffer = new byte[buf.capacity()];        
        
        // perform compression, and reorg
        // we assume that the keyPartitions are sorted by the prefixes (i.e., in the logical target order)
        int kpIndex = 0;
        int recSlotNum = 0;
        int prefixSlotNum = 0;
        numUncompressedRecords = 0;
        FieldPrefixFieldIterator recToWrite = new FieldPrefixFieldIterator(fields, frame);
        while(recSlotNum < numRecords) {           
            if(kpIndex < keyPartitions.size()) {
            	
            	// beginning of keyPartition found, compress entire keyPartition
            	if(recSlotNum == keyPartitions.get(kpIndex).firstRecSlotNum) {            		
            		
            		// number of fields we decided to use for compression of this keyPartition
            		int numFieldsToCompress = keyPartitions.get(kpIndex).maxPmiIndex + 1;            		
            		int segmentStart = keyPartitions.get(kpIndex).firstRecSlotNum;
            		int recordsInSegment = 1;
            		
            		//System.out.println("PROCESSING KEYPARTITION: " + kpIndex + " RANGE: " + keyPartitions.get(kpIndex).firstRecSlotNum + " " + keyPartitions.get(kpIndex).lastRecSlotNum + " FIELDSTOCOMPRESS: " + numFieldsToCompress);
            		
            		FieldPrefixFieldIterator prevRec = new FieldPrefixFieldIterator(fields, frame);        
                    FieldPrefixFieldIterator rec = new FieldPrefixFieldIterator(fields, frame);
                    
                    for(int i = recSlotNum + 1; i <= keyPartitions.get(kpIndex).lastRecSlotNum; i++) {
                    	prevRec.openRecSlotNum(i - 1);
                        rec.openRecSlotNum(i);
                        
                        // check if records match in numFieldsToCompress of their first fields
                        int prefixFieldsMatch = 0;
                        for(int j = 0; j < numFieldsToCompress; j++) {                                                    
                            if(cmps[j].compare(pageArray, prevRec.getFieldOff(), prevRec.getFieldSize(), pageArray, rec.getFieldOff(), rec.getFieldSize()) == 0) prefixFieldsMatch++;
                            else break;
                            prevRec.nextField();
                            rec.nextField();
                        }
                                                                  	                       
                        // the two records must match in exactly the number of fields we decided to compress for this keyPartition
                        int processSegments = 0;
                        if(prefixFieldsMatch == numFieldsToCompress) recordsInSegment++;                        
                        else processSegments++;

                        if(i == keyPartitions.get(kpIndex).lastRecSlotNum) processSegments++;
                        
                        for(int r = 0; r < processSegments; r++) {
                        	// compress current segment and then start new segment                       
                        	if(recordsInSegment < occurrenceThreshold || numFieldsToCompress <= 0) {
                        		// segment does not have at least occurrenceThreshold records, so write records uncompressed
                        		for(int j = 0; j < recordsInSegment; j++) {
                        		    int slotNum = segmentStart + j;
                        		    recToWrite.openRecSlotNum(slotNum);
                        		    newRecordSlots[numRecords - 1 - slotNum] = slotManager.encodeSlotFields(FieldPrefixSlotManager.RECORD_UNCOMPRESSED, recordFreeSpace);
                        		    recordFreeSpace += recToWrite.copyFields(0, fields.length - 1, buffer, recordFreeSpace);
                        		}
                        		numUncompressedRecords += recordsInSegment;
                        	}
                        	else {
                        	    // segment has enough records, compress segment
                        		// extract prefix, write prefix record to buffer, and set prefix slot                        	    
                        		newPrefixSlots[newPrefixSlots.length - 1 - prefixSlotNum] = slotManager.encodeSlotFields(numFieldsToCompress, prefixFreeSpace);
                        	    //int tmp = freeSpace;
                        	    //prevRec.reset();
                        	    //System.out.println("SOURCE CONTENTS: " + buf.getInt(prevRec.getFieldOff()) + " " + buf.getInt(prevRec.getFieldOff()+4));
                        	    prefixFreeSpace += prevRec.copyFields(0, numFieldsToCompress - 1, buffer, prefixFreeSpace);            	                            	                           	    
                        	    //System.out.println("WRITING PREFIX RECORD " + prefixSlotNum + " AT " + tmp + " " + freeSpace);
                        	    //System.out.print("CONTENTS: ");
                        	    //for(int x = 0; x < numFieldsToCompress; x++) System.out.print(buf.getInt(tmp + x*4) + " ");
                        	    //System.out.println();
                        	    
                        		// truncate records, write them to buffer, and set record slots
                        	    for(int j = 0; j < recordsInSegment; j++) {
                        	        int slotNum = segmentStart + j;
                                    recToWrite.openRecSlotNum(slotNum);
                                    newRecordSlots[numRecords - 1 - slotNum] = slotManager.encodeSlotFields(prefixSlotNum, recordFreeSpace);
                                    recordFreeSpace += recToWrite.copyFields(numFieldsToCompress, fields.length - 1, buffer, recordFreeSpace);                              
                                }
                        	    
                        	    prefixSlotNum++;
                        	}
                        	
                        	// begin new segment
                        	segmentStart = i;
                        	recordsInSegment = 1;               	
                        }                   
                    }
                    
                    recSlotNum = keyPartitions.get(kpIndex).lastRecSlotNum;
                	kpIndex++; 	
                }
            	else {
                    // just write the record uncompressed
            	    recToWrite.openRecSlotNum(recSlotNum);
            	    newRecordSlots[numRecords - 1 - recSlotNum] = slotManager.encodeSlotFields(FieldPrefixSlotManager.RECORD_UNCOMPRESSED, recordFreeSpace);
            	    recordFreeSpace += recToWrite.copyFields(0, fields.length - 1, buffer, recordFreeSpace);
            	    numUncompressedRecords++;
            	}
            }
            else {
                // just write the record uncompressed
                recToWrite.openRecSlotNum(recSlotNum);
                newRecordSlots[numRecords - 1 - recSlotNum] = slotManager.encodeSlotFields(FieldPrefixSlotManager.RECORD_UNCOMPRESSED, recordFreeSpace);
                recordFreeSpace += recToWrite.copyFields(0, fields.length - 1, buffer, recordFreeSpace);
                numUncompressedRecords++;
            }   
            recSlotNum++;
        }            
        
        // sanity check to see if we have written exactly as many prefix bytes as computed before
        if(prefixFreeSpace != frame.getOrigFreeSpaceOff() + totalPrefixBytes) {
        	throw new Exception("ERROR: Number of prefix bytes written don't match computed number");
        }
                
        // in some rare instances our procedure could even increase the space requirement which is very dangerous
        // this can happen to to the greedy solution of the knapsack-like problem
        // therefore, we check if the new space exceeds the page size to avoid the only danger of an increasing space
        int totalSpace = recordFreeSpace + newRecordSlots.length * slotManager.getSlotSize() + newPrefixSlots.length * slotManager.getSlotSize();
        if(totalSpace > buf.capacity()) return false; // just leave the page as is
        
        // copy new records and new slots into original page
        int freeSpaceAfterInit = frame.getOrigFreeSpaceOff();
        System.arraycopy(buffer, freeSpaceAfterInit, pageArray, freeSpaceAfterInit, recordFreeSpace - freeSpaceAfterInit);
        
        // copy prefix slots
        int slotOffRunner = buf.capacity() - slotManager.getSlotSize();
        for(int i = 0; i < newPrefixSlots.length; i++) {
            buf.putInt(slotOffRunner, newPrefixSlots[newPrefixSlots.length - 1 - i]);
            slotOffRunner -= slotManager.getSlotSize();
        }
        
        // copy record slots
        for(int i = 0; i < newRecordSlots.length; i++) {
            buf.putInt(slotOffRunner, newRecordSlots[newRecordSlots.length - 1 - i]);
            slotOffRunner -= slotManager.getSlotSize();
        }
        
//        int originalFreeSpaceOff = frame.getOrigFreeSpaceOff();   
//        System.out.println("ORIGINALFREESPACE: " + originalFreeSpaceOff);
//        System.out.println("RECSPACE BEF: " + (frame.getFreeSpaceOff() - originalFreeSpaceOff));
//        System.out.println("RECSPACE AFT: " + (recordFreeSpace - originalFreeSpaceOff));
//        System.out.println("PREFIXSLOTS BEF: " + frame.getNumPrefixRecords());
//        System.out.println("PREFIXSLOTS AFT: " + newPrefixSlots.length);
//        
//        System.out.println("FREESPACE BEF: " + frame.getFreeSpaceOff());
//        System.out.println("FREESPACE AFT: " + recordFreeSpace);
//        System.out.println("PREFIXES: " + newPrefixSlots.length + " / " + FieldPrefixSlotManager.MAX_PREFIX_SLOTS);
//        System.out.println("RECORDS: " + newRecordSlots.length);
        
        // update space fields, TODO: we need to update more fields 
        frame.setFreeSpaceOff(recordFreeSpace);
        frame.setNumPrefixRecords(newPrefixSlots.length);
        frame.setNumUncompressedRecords(numUncompressedRecords);
        int totalFreeSpace = buf.capacity() - recordFreeSpace - ((newRecordSlots.length + newPrefixSlots.length) * slotManager.getSlotSize());
        frame.setTotalFreeSpace(totalFreeSpace);
        
        return true;
    }
	
    // we perform an analysis pass over the records to determine the costs and benefits of different compression options
    // a "keypartition" is a range of records that has an identical first field
    // for each keypartition we chose a prefix length to use for compression
    // i.e., all records in a keypartition will be compressed based on the same prefix length (number of fields)
    // the prefix length may be different for different keypartitions
    // the occurrenceThreshold determines the minimum number of records that must share a common prefix in order for us to consider compressing them        
    private ArrayList<KeyPartition> getKeyPartitions(FieldPrefixNSMLeafFrame frame, MultiComparator cmp, int occurrenceThreshold) {        
    	IBinaryComparator[] cmps = cmp.getComparators();
        IFieldAccessor[] fields = cmp.getFields();
        
        int maxCmps = cmps.length - 1;
        ByteBuffer buf = frame.getBuffer();
        byte[] pageArray = buf.array();
        IPrefixSlotManager slotManager = frame.slotManager;
        
        ArrayList<KeyPartition> keyPartitions = new ArrayList<KeyPartition>();        
        KeyPartition kp = new KeyPartition(maxCmps);        
        keyPartitions.add(kp);
        
        FieldPrefixFieldIterator prevRec = new FieldPrefixFieldIterator(fields, frame);        
        FieldPrefixFieldIterator rec = new FieldPrefixFieldIterator(fields, frame);
        
        kp.firstRecSlotNum = 0;        
        int numRecords = frame.getNumRecords();
        for(int i = 1; i < numRecords; i++) {        	        	
        	prevRec.openRecSlotNum(i-1);
        	rec.openRecSlotNum(i);
        	
            //System.out.println("BEFORE RECORD: " + i + " " + rec.recSlotOff + " " + rec.recOff);
            //kp.print();
            
            int prefixFieldsMatch = 0;            
            int prefixBytes = 0; // counts the bytes of the common prefix fields
            
            for(int j = 0; j < maxCmps; j++) {                
            	            	
            	if(cmps[j].compare(pageArray, prevRec.getFieldOff(), prevRec.getFieldSize(), pageArray, rec.getFieldOff(), prevRec.getFieldSize()) == 0) {
                    prefixFieldsMatch++;
                    kp.pmi[j].matches++;                     
                    prefixBytes += rec.getFieldSize();            
                    
                    if(kp.pmi[j].matches == occurrenceThreshold) {
                        // if we compress this prefix, we pay the cost of storing it once, plus the size for one prefix slot
                        kp.pmi[j].prefixBytes += prefixBytes;
                        kp.pmi[j].spaceCost += prefixBytes + slotManager.getSlotSize();
                        kp.pmi[j].prefixSlotsNeeded++;
                        kp.pmi[j].spaceBenefit += occurrenceThreshold*prefixBytes;                        
                    }
                    else if(kp.pmi[j].matches > occurrenceThreshold) {
                        // we are beyond the occurrence threshold, every additional record with a matching prefix increases the benefit 
                        kp.pmi[j].spaceBenefit += prefixBytes;
                    }
                }
                else {
                    kp.pmi[j].matches = 1;     
                    break;
                }         
                
                prevRec.nextField();
                rec.nextField();
            }
            
            //System.out.println();
            //System.out.println("AFTER RECORD: " + i);
            //kp.print();
            //System.out.println("-----------------");
                        
            // this means not even the first field matched, so we start to consider a new "key partition"
            if(maxCmps > 0 && prefixFieldsMatch == 0) {                
            	//System.out.println("NEW KEY PARTITION");            	
            	kp.lastRecSlotNum = i-1;
            	
                // remove keyPartitions that don't have enough records
                if((kp.lastRecSlotNum - kp.firstRecSlotNum) + 1 < occurrenceThreshold) keyPartitions.remove(keyPartitions.size() - 1);
                                
            	kp = new KeyPartition(maxCmps);
                keyPartitions.add(kp);
                kp.firstRecSlotNum = i;
            }         
        }   
        kp.lastRecSlotNum = numRecords - 1;
        // remove keyPartitions that don't have enough records
        if((kp.lastRecSlotNum - kp.firstRecSlotNum) + 1 < occurrenceThreshold) keyPartitions.remove(keyPartitions.size() - 1);
        
        return keyPartitions;
    }
    
        
    private class PrefixMatchInfo {
        public int matches = 1;
        public int spaceCost = 0;      
        public int spaceBenefit = 0;
        public int prefixSlotsNeeded = 0;     
        public int prefixBytes = 0;
    }
    
    private class KeyPartition {        
        public int firstRecSlotNum;
        public int lastRecSlotNum;
        public PrefixMatchInfo[] pmi;
        
        public int maxBenefitMinusCost = 0;
        public int maxPmiIndex = -1;        
        // number of fields used for compression for this kp of current page
        
        public KeyPartition(int numKeyFields) {
            pmi = new PrefixMatchInfo[numKeyFields];
            for(int i = 0; i < numKeyFields; i++) {
                pmi[i] = new PrefixMatchInfo();
            }
        }        
    }    
    
    private class SortByHeuristic implements Comparator<KeyPartition>{
        @Override
        public int compare(KeyPartition a, KeyPartition b) {
            if(a.maxPmiIndex < 0) {
                if(b.maxPmiIndex < 0) return 0;
                return 1;
            } else if(b.maxPmiIndex < 0) return -1;
            
            // non-negative maxPmiIndex, meaning a non-zero benefit exists
            float thisHeuristicVal = (float)a.maxBenefitMinusCost / (float)a.pmi[a.maxPmiIndex].prefixSlotsNeeded; 
            float otherHeuristicVal = (float)b.maxBenefitMinusCost / (float)b.pmi[b.maxPmiIndex].prefixSlotsNeeded;
            if(thisHeuristicVal < otherHeuristicVal) return 1;
            else if(thisHeuristicVal > otherHeuristicVal) return -1;
            else return 0;
        }
    }
    
    private class SortByOriginalRank implements Comparator<KeyPartition>{            
        @Override
        public int compare(KeyPartition a, KeyPartition b) {
            return a.firstRecSlotNum - b.firstRecSlotNum;
        }
    }
}
