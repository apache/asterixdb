/*
 * Copyright 2013 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;

import edu.uci.ics.asterix.transaction.management.service.locking.AllocInfo;
import edu.uci.ics.asterix.transaction.management.service.locking.RecordManagerTypes;

public class @E@ArenaManager {
    
    public static final boolean TRACK_ALLOC = @DEBUG@;

    private final int noArenas;
    private final long txnShrinkTimer;
    private ArrayList<@E@RecordManager> arenas;
    private volatile int nextArena; 
    private ThreadLocal<LocalManager> local;    
    
    public @E@ArenaManager(long txnShrinkTimer) {
        this.txnShrinkTimer = txnShrinkTimer;
        noArenas = Runtime.getRuntime().availableProcessors() * 2;
        arenas = new ArrayList<@E@RecordManager>(noArenas);
        nextArena = 0;
        local = new ThreadLocal<LocalManager>() {
            @Override
            protected LocalManager initialValue() {
                return getNext();
            }
        };
    }

    public long allocate() {
        final LocalManager localManager = local.get();
        long result = localManager.arenaId;
        result = result << 48;
        final int localId = localManager.mgr.allocate();
        result |= localId;
        if (TRACK_ALLOC) {
            final long allocId = (++localManager.mgr.allocCounter % 0x7fff);
            result |= (allocId << 32);
            setAllocId(result, (short) allocId);
            assert RecordManagerTypes.Global.allocId(result) == allocId;
        }
        assert RecordManagerTypes.Global.arenaId(result) == localManager.arenaId;
        assert RecordManagerTypes.Global.localId(result) == localId;
        return result;
    }
    
    public void deallocate(long slotNum) {
        if (TRACK_ALLOC) {
            checkSlot(slotNum);
        }
        final int arenaId = RecordManagerTypes.Global.arenaId(slotNum);
        get(arenaId).deallocate(RecordManagerTypes.Global.localId(slotNum));
    }
    
    public synchronized LocalManager getNext() {
        if (nextArena >= arenas.size()) { 
            arenas.add(new @E@RecordManager(txnShrinkTimer));
        }
        @E@RecordManager mgr = arenas.get(nextArena);
        LocalManager res = new LocalManager();
        res.mgr = mgr;
        res.arenaId = nextArena;
        nextArena = (nextArena + 1) % noArenas;
        return res;
    }
    
    public @E@RecordManager get(int i) {
        return arenas.get(i);
    }
    
    public @E@RecordManager local() {
        return local.get().mgr;
    }
    
    @METHODS@
    
    private void checkSlot(long slotNum) {
        final int refAllocId = RecordManagerTypes.Global.allocId(slotNum);
        final short curAllocId = getAllocId(slotNum);
        if (refAllocId != curAllocId) {
            String msg = "reference to slot " + slotNum
                + " of arena " + RecordManagerTypes.Global.arenaId(slotNum)
                + " refers to version " + Integer.toHexString(refAllocId)
                + " current version is " + Integer.toHexString(curAllocId);
            AllocInfo a = getAllocInfo(slotNum);
            if (a != null) {
                msg += "\n" + a.toString();
            }
            throw new IllegalStateException(msg);
        }
    }
    
    public AllocInfo getAllocInfo(long slotNum) {
        final int arenaId = RecordManagerTypes.Global.arenaId(slotNum);
        return get(arenaId).getAllocInfo(RecordManagerTypes.Global.localId(slotNum));
    }
    
    static class LocalManager {
        int arenaId;
        @E@RecordManager mgr;
    }
    
    public StringBuilder append(StringBuilder sb) {
        for (int i = 0; i < arenas.size(); ++i) {
            sb.append("++++ arena ").append(i).append(" ++++\n");
            arenas.get(i).append(sb);
        }
        return sb;
    }
    
    public String toString() {
        return append(new StringBuilder()).toString();
    }

    public Stats addTo(Stats s) {
        final int size = arenas.size();
        s.arenas += size;
        for (int i = 0; i < size; ++i) {
            arenas.get(i).addTo(s);
        }
        return s;
    }
}
