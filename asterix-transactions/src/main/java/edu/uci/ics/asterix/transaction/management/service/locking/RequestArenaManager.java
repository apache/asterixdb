package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;

public class RequestArenaManager {
    
    private final int noArenas;
    private ArrayList<RequestMemoryManager> arenas;
    private volatile int nextArena; 
    private ThreadLocal<LocalManager> local;    
    
    public RequestArenaManager() {
        noArenas = Runtime.getRuntime().availableProcessors() * 2;
        arenas = new ArrayList<RequestMemoryManager>(noArenas);
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
            arenas.add(new RequestMemoryManager());
        }
        RequestMemoryManager mgr = arenas.get(nextArena);
        LocalManager res = new LocalManager();
        res.mgr = mgr;
        res.arenaId = nextArena;
        nextArena = (nextArena + 1) % noArenas;
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

