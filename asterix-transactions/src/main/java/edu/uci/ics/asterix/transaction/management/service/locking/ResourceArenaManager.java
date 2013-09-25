package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;

public class ResourceArenaManager {
    
    private ArrayList<ResourceMemoryManager> arenas;
    private volatile int nextArena; 
    private ThreadLocal<LocalManager> local;    
    
    public ResourceArenaManager() {
        int noArenas = Runtime.getRuntime().availableProcessors() * 2;
        arenas = new ArrayList<ResourceMemoryManager>(noArenas);
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
        if (nextArena >= arenas.size()) { 
            arenas.add(new ResourceMemoryManager());
        }
        ResourceMemoryManager mgr = arenas.get(nextArena);
        LocalManager res = new LocalManager();
        res.mgr = mgr;
        res.arenaId = nextArena;
        nextArena = (nextArena + 1) % arenas.size();
        return res;
    }
    
    public ResourceMemoryManager get(int i) {
        return arenas.get(i);
    }
    
    public ResourceMemoryManager local() {
        return local.get().mgr;
    }
    
    public int getLastHolder(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getLastHolder(localId(slotNum));
    }

    public void setLastHolder(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setLastHolder(localId(slotNum), value);
    }

    public int getFirstWaiter(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getFirstWaiter(localId(slotNum));
    }

    public void setFirstWaiter(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setFirstWaiter(localId(slotNum), value);
    }

    public int getFirstUpgrader(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getFirstUpgrader(localId(slotNum));
    }

    public void setFirstUpgrader(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setFirstUpgrader(localId(slotNum), value);
    }

    public int getMaxMode(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getMaxMode(localId(slotNum));
    }

    public void setMaxMode(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setMaxMode(localId(slotNum), value);
    }

    public int getDatasetId(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getDatasetId(localId(slotNum));
    }

    public void setDatasetId(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setDatasetId(localId(slotNum), value);
    }

    public int getPkHashVal(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getPkHashVal(localId(slotNum));
    }

    public void setPkHashVal(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setPkHashVal(localId(slotNum), value);
    }

    public int getNext(int slotNum) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        return local.get().mgr.getNext(localId(slotNum));
    }

    public void setNext(int slotNum, int value) {
        final int arenaId = arenaId(slotNum);
        if (arenaId != local.get().arenaId) {
            local.get().arenaId = arenaId;
            local.get().mgr = get(arenaId);
        }
        local.get().mgr.setNext(localId(slotNum), value);
    }

    
    static class LocalManager {
        int arenaId;
        ResourceMemoryManager mgr;
    }
}

