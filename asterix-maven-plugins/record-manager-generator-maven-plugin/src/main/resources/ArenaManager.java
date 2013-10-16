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

import edu.uci.ics.asterix.transaction.management.service.locking.@E@RecordManager.Buffer.Alloc;

public class @E@ArenaManager {
    
    public static final boolean TRACK_ALLOC = true;

    private final int noArenas;
    private ArrayList<@E@RecordManager> arenas;
    private volatile int nextArena; 
    private ThreadLocal<LocalManager> local;    
    
    public @E@ArenaManager() {
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
    
    public static int arenaId(long l) {
        return (int)((l >>> 48) & 0xffff);
    }

    public static int allocId(long l) {
        return (int)((l >>> 32) & 0xffff);
    }

    public static int localId(long l) {
        return (int) (l & 0xffffffffL);
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
            assert allocId(result) == allocId;
        }
        assert arenaId(result) == localManager.arenaId;
        assert localId(result) == localId;
        return result;
    }
    
    public void deallocate(long slotNum) {
        if (TRACK_ALLOC) {
            checkSlot(slotNum);
        }
        final int arenaId = arenaId(slotNum);
        get(arenaId).deallocate(localId(slotNum));
    }
    
    public synchronized LocalManager getNext() {
        if (nextArena >= arenas.size()) { 
            arenas.add(new @E@RecordManager());
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
        final int refAllocId = allocId(slotNum);
        final short curAllocId = getAllocId(slotNum);
        if (refAllocId != curAllocId) {
            System.err.println("checkSlot(" + slotNum + "): " + refAllocId);
            String msg = "reference to slot " + slotNum
                + " of arena " + arenaId(slotNum) + " refers to version " 
                + Integer.toHexString(refAllocId) + " current version is "
                + Integer.toHexString(curAllocId);
            Alloc a = getAlloc(slotNum);
            if (a != null) {
                msg += "\n" + a.toString();
            }
            throw new IllegalStateException(msg);
        }
    }
    
    public Alloc getAlloc(long slotNum) {
        final int arenaId = arenaId(slotNum);
        return get(arenaId).getAlloc(localId(slotNum));
    }
    
    static class LocalManager {
        int arenaId;
        @E@RecordManager mgr;
    }
    
    StringBuffer append(StringBuffer sb) {
        for (int i = 0; i < arenas.size(); ++i) {
            sb.append("++++ arena ").append(i).append(" ++++\n");
            arenas.get(i).append(sb);
        }
        return sb;
    }
    
    public String toString() {
        return append(new StringBuffer()).toString();
    }
}
