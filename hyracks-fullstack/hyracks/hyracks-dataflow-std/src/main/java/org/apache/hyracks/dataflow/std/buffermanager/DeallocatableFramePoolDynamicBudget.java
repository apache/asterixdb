package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;

import java.nio.ByteBuffer;

public class DeallocatableFramePoolDynamicBudget extends DeallocatableFramePool{

    int desiredBuffer = Integer.MAX_VALUE;
    public DeallocatableFramePoolDynamicBudget(IHyracksFrameMgrContext ctx, int memBudgetInBytes) {
        super(ctx, memBudgetInBytes);
    }

    @Override
    public void deAllocateBuffer(ByteBuffer buffer) {
        if (buffer.capacity() != ctx.getInitialFrameSize() || desiredBuffer < allocated) {
            // simply deallocate the Big Object frame
            ctx.deallocateFrames(buffer.capacity());
            int framesReleased = buffer.capacity();
            if(desiredBuffer < allocated){
                memBudget = memBudget - framesReleased;
                memBudget = memBudget < desiredBuffer ? desiredBuffer : memBudget;
            }
            allocated -= framesReleased;
        } else {
            buffers.add(buffer);
        }
    }

    @Override
    public boolean updateMemoryBudget(int newBudget){
        desiredBuffer = newBudget * ctx.getInitialFrameSize();
        if(this.allocated < newBudget){
            this.memBudget = desiredBuffer; //Simply Update Memory Budget
            return true;
        }
        return false; //There are more alocated Frames then the new Budget.
    }

}
