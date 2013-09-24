package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;

public class RequestArenaManager {
    
    private ArrayList<RequestMemoryManager> arenas;
    private volatile int nextArena; 
    private ThreadLocal<LocalManager> local;    
    
    public RequestArenaManager() {
        int noArenas = Runtime.getRuntime().availableProcessors() * 2;
        arenas = new ArrayList<RequestMemoryManager>(noArenas);
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

    public synchronized LocalManager getNext() {
        RequestMemoryManager mgr = arenas.get(nextArena);
        if (mgr == null) {
            mgr = new RequestMemoryManager();
            arenas.set(nextArena, mgr);
        }
        LocalManager res = new LocalManager();
        res.mgr = mgr;
        res.arenaId = nextArena;
        nextArena = (nextArena + 1) % arenas.size();
        return res;
    }
    
    public RequestMemoryManager get(int i) {
        return arenas.get(i);
    }
    
    public RequestMemoryManager local() {
        return local.get().mgr;
    }
    
    public int getResourceId(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getResourceId(localId(slotNum));
    }

    public void setResourceId(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setResourceId(localId(slotNum), value);
    }

    public int getLockMode(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getLockMode(localId(slotNum));
    }

    public void setLockMode(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setLockMode(localId(slotNum), value);
    }

    public int getJobId(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getJobId(localId(slotNum));
    }

    public void setJobId(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setJobId(localId(slotNum), value);
    }

    public int getPrevJobRequest(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getPrevJobRequest(localId(slotNum));
    }

    public void setPrevJobRequest(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setPrevJobRequest(localId(slotNum), value);
    }

    public int getNextJobRequest(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getNextJobRequest(localId(slotNum));
    }

    public void setNextJobRequest(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setNextJobRequest(localId(slotNum), value);
    }

    public int getNextRequest(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getNextRequest(localId(slotNum));
    }

    public void setNextRequest(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setNextRequest(localId(slotNum), value);
    }

    
    static class LocalManager {
        int arenaId;
        RequestMemoryManager mgr;
    }
}

