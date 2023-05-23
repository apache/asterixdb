package org.apache.hyracks.dataflow.std.join;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class MemoryChangeStats {
    List<MemoryStatEvent> events = new ArrayList<MemoryStatEvent>();
    int partitions[];
    String relationName;
    BitSet spilledPartitions;
    BitSet inconsistentPartitions;
    BitSet hashedPartitions;
    Phase phase;
    int currentBudget = 0;
    long currentFreeMemory = 0;
    int currentFramesProcessed;


    public enum Phase{
        BUILD,
        PROBE
    }

    private enum eventType {
        Contention,
        Expansion,
        Spill,
        Reload,

    }

    /**
     *
     * @param phase False -> Build
     * @param relationSizeInTuples
     */
    public void SetOperatorInfo(Phase phase,int relationSizeInTuples[],BitSet spilledPartitions,BitSet inconsistentPartitions,String relationName,BitSet hashedPartitions){
        this.partitions = relationSizeInTuples;
        this.relationName = relationName;
        this.phase = phase;
        this.spilledPartitions = spilledPartitions;
        this.inconsistentPartitions = inconsistentPartitions;
        this.hashedPartitions = hashedPartitions;

    }
    private class MemoryStatEvent {
        int memoryBudget = currentBudget;
        Integer memoryUpdate = null;
        int bytesSpilled = 0;
        int spillEvents = 0;
        int processedFrames = currentFramesProcessed;
        private Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
        private Integer partitionsReloaded = null;
        private Integer spilledPartition = null;
        private Integer reloadedPartition = null;
        private long freeMemory = currentFreeMemory;
        private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        private eventType eventType;


        public void UpdateBytesSpilled(int bytesSpilled){
            this.bytesSpilled += bytesSpilled;
            this.spillEvents++;
        }

        /**
         * Return a printable Memory Stat
         * @return
         */
        private String printableMemStatEvent(){
            return String.format("|%15s|%10s|%10d|%15s|%15d|%20d|%15s|\n",
                    sdf.format(this.timeStamp) ,
                    number2String(memoryUpdate,true),
                    memoryBudget,
                    number2String(spilledPartition,false),
                    spillEvents,
                    processedFrames,
                    number2String(reloadedPartition,false));
        }
    }
    public void AddContentionEvent(int oldBudget,int newBudget){
        MemoryStatEvent event = new MemoryStatEvent();
        event.memoryBudget = newBudget;
        event.memoryUpdate = oldBudget != 0 ? newBudget - oldBudget:null;
        this.currentBudget = newBudget;
        this.events.add(event);
    }

    public void AddSpillingEvent(int pId,long currentFreeMemory){
        MemoryStatEvent event = new MemoryStatEvent();
        event.spilledPartition = pId;
        this.currentFreeMemory = currentFreeMemory;
        this.events.add(event);
    }

    public void AddReloadEvent(int pId,long currentFreeMemory){
        MemoryStatEvent event = new MemoryStatEvent();
        event.reloadedPartition = pId;
        this.currentFreeMemory = currentFreeMemory;
        this.events.add(event);
    }

    public void UpdateProcessedFrames(int frames){
        currentFramesProcessed =frames;
    }
    public String PrintableMemBudgetStat(){
        String ret =PrintableOperatorStats();
        ret += " --------------------------------------------------------------------------------------------------------------- \n";
        ret += "|   Timestamp   | Variation|  Budget  | Spilled Part. |  Spiling Calls |  Frames Processed  |Reloaded Part. |\n";
        ret += "|---------------------------------------------------------------------------------------------------------------|\n";
        for(MemoryStatEvent e : events){
            ret += e.printableMemStatEvent();
        }
        ret += " ---------------------------------------------------------------------------------------------------------------\n";
        return ret;
    }
    private String PrintableOperatorStats(){
        String ret ="\n";
        ret += " --------------------------------------------------------------------------------------------------------------- \n";
        ret += String.format("                                       %s - %s\n",phase == Phase.BUILD ? "BUILD" : "PROBE",relationName);
        ret += String.format("Partition Size in Tuples: (%d)[", Arrays.stream(partitions).sum());
        ret+= StringUtils.join(ArrayUtils.toObject(partitions), ",");
        ret+= "]\n";
        ret+= "Spilled Partitions: " + spilledPartitions.toString() + "\n";
        ret+= "Inconsistent Partitions: " + inconsistentPartitions.toString() + "\n";
        ret+= "Hashed Partitions: " + hashedPartitions.toString() + "\n";
        return ret;
    }

    private String number2String(Integer number,boolean withSign){
        if(withSign)
            return number != null ? String.format("%+d",number) : "";
        return number != null ? number.toString() : "";
    }
    public void clear(){
        this.events.clear();
    }

}
