package edu.uci.ics.hyracks.control.nc.resources.memory;

import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.hyracks.api.resources.memory.IMemoryManager;

public class MemoryManager implements IMemoryManager {
    private final long maxMemory;

    private final AtomicLong memory;

    public MemoryManager(long maxMemory) {
        this.maxMemory = maxMemory;
        this.memory = new AtomicLong(maxMemory);
    }

    @Override
    public long getMaximumMemory() {
        return maxMemory;
    }

    @Override
    public long getAvailableMemory() {
        return memory.get();
    }

    @Override
    public boolean allocate(long memory) {
        // commented as now the deallocation is not implemented yet.
        //        if (this.memory.addAndGet(-memory) < 0) {
        //            this.memory.addAndGet(memory);
        //            return false;
        //        }
        return true;
    }

    @Override
    public void deallocate(long memory) {
        this.memory.addAndGet(memory);
    }
}