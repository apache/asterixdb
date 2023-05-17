package org.apache.hyracks.dataflow.std.join;

import java.util.ArrayList;
import java.util.List;

public class MemoryChangeStats {
    List<MemoryStatEvent> events = new ArrayList<MemoryStatEvent>();

    private class MemoryStatEvent {
        int memoryBudget;
        int memoryUpdate;
        int bytesSpilled = 0;
        int spillEvents = 0;
        int processedFrames = 0;
        public MemoryStatEvent(int oldMemoryBudget,int newMemoryBudget,int bytesSpilled,int spillEvents,int processedFrames){
            this.memoryBudget = newMemoryBudget;
            this.memoryUpdate = newMemoryBudget - oldMemoryBudget;
            this.bytesSpilled = bytesSpilled;
            this.spillEvents = spillEvents;
            this.processedFrames = processedFrames;
        }

        public void UpdateBytesSpilled(int bytesSpilled){
            this.bytesSpilled += bytesSpilled;
            this.spillEvents++;
        }

        /**
         * Return a printable Memory Stat
         * @return
         */
        private String printableMemStatEvent(){
            return String.format("|%10d|%10d|%10d|%10d|\n", memoryBudget,bytesSpilled,spillEvents,processedFrames);
        }
    }

    public void AddContentionEvent(int oldBudget,int newBudget){
        int bytesSpilled = 0;
        int spillEvents =0;
        int processedFrames =0;
        if(this.events.size() > 1) {
            MemoryStatEvent lastEvent = this.events.get(this.events.size() - 1);
            bytesSpilled = lastEvent.bytesSpilled;
            spillEvents =lastEvent.spillEvents;
            processedFrames =lastEvent.processedFrames;
        }
        this.events.add(new MemoryStatEvent(oldBudget,newBudget,bytesSpilled,spillEvents,processedFrames));
    }

    public void UpdateSpilled(int spilled){
        MemoryStatEvent e = this.events.get(this.events.size()-1);
        e.UpdateBytesSpilled(spilled);
    }
    public void UpdateProcessedFrames(int frames){
        MemoryStatEvent e = this.events.get(this.events.size()-1);
        e.processedFrames = frames;
    }

    public String PrintableMemBudgetStat(){
        String ret = String.format("\n|%10s|%10s|%10s|%10s|\n"," Budget"," Pages Spilled","Spiling Calls"," Frames Processed");
        for(MemoryStatEvent e : events){
            ret += e.printableMemStatEvent();
        }
        return ret;
    }

}
