package edu.uci.ics.hyracks.api.resources.memory;

public interface IMemoryManager {
    public long getMaximumMemory();

    public long getAvailableMemory();

    public boolean allocate(long memory);

    public void deallocate(long memory);
}