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

public class @E@ArenaManager {
    
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
    
    public static int arenaId(int i) {
        return (i >> 24) & 0xff;
    }

    public static int localId(int i) {
        return i & 0xffffff;
    }

    public int allocate() {
        final LocalManager localManager = local.get();
        int result = localManager.arenaId << 24;
        result |= localManager.mgr.allocate();
        return result;
    }
    
    public void deallocate(int slotNum) {
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
