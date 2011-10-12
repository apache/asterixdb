package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionGeneratorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerGeneratorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerGeneratorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RepartitionComputerGeneratorFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractTaskState;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;



public class OptimizedHybridHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
	
	private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;
	
	private static final long serialVersionUID = 1L;
	private static final double NLJ_SWITCH_THRESHOLD = 0.8;
	
    
    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";
    
    private final int memsize;
	private final int inputsize0;
	private final double factor;
    private final int[] probeKeys;
    private final int[] buildKeys;
    private final IBinaryHashFunctionGeneratorFactory[] hashFunctionGeneratorFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;			//For in-mem HJ
    private final ITuplePairComparatorFactory tuplePairComparatorFactory0;	//For NLJ in probe
    private final ITuplePairComparatorFactory tuplePairComparatorFactory1;	//For NLJ in probe
    
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;
    
    public OptimizedHybridHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionGeneratorFactory[] hashFunctionGeneratorFactories, 
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, ITuplePairComparatorFactory tupPaircomparatorFactory0, ITuplePairComparatorFactory tupPaircomparatorFactory1, 
            boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1) throws HyracksDataException  {
       
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.factor = factor;
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
        this.comparatorFactories = comparatorFactories;
        this.tuplePairComparatorFactory0 = tupPaircomparatorFactory0;
        this.tuplePairComparatorFactory1 = tupPaircomparatorFactory1;
        recordDescriptors[0] = recordDescriptor;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
        
    }
    
    public OptimizedHybridHashJoinOperatorDescriptor(JobSpecification spec, int memsize,int inputsize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionGeneratorFactory[] hashFunctionGeneratorFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, ITuplePairComparatorFactory tupPaircomparatorFactory0, ITuplePairComparatorFactory tupPaircomparatorFactory1)
            throws HyracksDataException  {
       
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.factor = factor;
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
        this.comparatorFactories = comparatorFactories;
        this.tuplePairComparatorFactory0 = tupPaircomparatorFactory0;
        this.tuplePairComparatorFactory1 = tupPaircomparatorFactory1;
        recordDescriptors[0] = recordDescriptor;
        this.isLeftOuter = false;
        this.nullWriterFactories1 = null;
    }
    
    @Override
	public void contributeActivities(IActivityGraphBuilder builder) {
    	PartitionAndBuildActivityNode phase1 = new PartitionAndBuildActivityNode(new ActivityId(odId,
                BUILD_AND_PARTITION_ACTIVITY_ID));
    	ProbeAndJoinActivityNode phase2 = new ProbeAndJoinActivityNode(new ActivityId(odId,
                PARTITION_AND_JOIN_ACTIVITY_ID));

        builder.addActivity(phase1);
        builder.addSourceEdge(0, phase1, 0);

        builder.addActivity(phase2);
        builder.addSourceEdge(1, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
		
	}
    
    
    	//memorySize is the memory for join (we have already excluded the 2 buffers for in/out)
    private int getNumberOfPartitions(int memorySize, int buildSize, double factor, int nPartitions) throws HyracksDataException{
    	int B = 0;
    	if (memorySize > 1) {
            if (memorySize > buildSize) {
               return 1;		//We will switch to in-Mem HJ eventually
            }
            else {
                B = (int) (Math.ceil((double) (buildSize * factor / nPartitions - memorySize)
                        / (double) (memorySize - 1)));
                if (B <= 0) {
                    B = 1;			//becomes in-memory hash join
                }
            }
            if(B > memorySize){
            	B = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            	return ( B<memorySize ? B : memorySize );
            }
        }
    	
    	else {
            throw new HyracksDataException("not enough memory for Hybrid Hash Join");
        }

    	return B;
    }
    
    public static class BuildAndPartitionTaskState extends AbstractTaskState {
        
    	private int memForJoin;
    	private int numOfPartitions;
    	private OptimizedHybridHashJoin hybridHJ;
        
        public BuildAndPartitionTaskState() {
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }
    
    
    private class PartitionAndBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        
        
        public PartitionAndBuildActivityNode(ActivityId id) {
            super(id);
        }
        
        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
            
        	
        	
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            
            final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
                }
            }
            
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
            	 private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(ctx.getJobletContext()
                         .getJobId(), new TaskId(getActivityId(), partition));
            	
            	ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerGeneratorFactory(probeKeys, hashFunctionGeneratorFactories).createPartitioner(0);
            	ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerGeneratorFactory(buildKeys, hashFunctionGeneratorFactories).createPartitioner(0);
            	
            	
                @Override
                public void open() throws HyracksDataException {
                	
                	if(memsize > 2){
                		state.memForJoin = memsize - 2;
                		state.numOfPartitions = getNumberOfPartitions(state.memForJoin, inputsize0, factor, nPartitions);
                		state.hybridHJ = new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                				PROBE_REL, BUILD_REL, probeKeys, buildKeys, comparators, 
                				probeRd, buildRd, probeHpc, buildHpc);
                		state.hybridHJ.initBuild();
                	}
                	else{
                		throw new HyracksDataException("not enough memory for Hybrid Hash Join");
                	}
                	
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.build(buffer);
                }
                
                @Override
                public void close() throws HyracksDataException {
                	state.hybridHJ.closeBuild();
                	env.setTaskState(state);
                }

				@Override
				public void fail() throws HyracksDataException {
				}
                
            };
            return op;
        }
    }
    
    private class ProbeAndJoinActivityNode extends AbstractActivityNode {

    	private static final long serialVersionUID = 1L;
    	private int maxLevel = -1;
    	
    	public ProbeAndJoinActivityNode(ActivityId id){
    		super(id);
    	}

    	@Override
    	public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
    			IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {

    		final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
    		final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
    		final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
    		final ITuplePairComparator nljComparator0 = tuplePairComparatorFactory0.createTuplePairComparator();
    		final ITuplePairComparator nljComparator1 = tuplePairComparatorFactory1.createTuplePairComparator();

    		for (int i = 0; i < comparatorFactories.length; ++i) {
    			comparators[i] = comparatorFactories[i].createBinaryComparator();
    		}

    		final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
    		if (isLeftOuter) {
    			for (int i = 0; i < nullWriterFactories1.length; i++) {
    				nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
    			}
    		}

    		IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
    			private BuildAndPartitionTaskState state;
    			private ByteBuffer rPartbuff = ctx.allocateFrame();
    			
    			private ITuplePartitionComputerGeneratorFactory hpcf0 = new FieldHashPartitionComputerGeneratorFactory(probeKeys, hashFunctionGeneratorFactories);
    			private ITuplePartitionComputerGeneratorFactory hpcf1 = new FieldHashPartitionComputerGeneratorFactory(buildKeys, hashFunctionGeneratorFactories);
    			
    			private ITuplePartitionComputer hpcRep0;
    			private ITuplePartitionComputer hpcRep1;
    			
    			@Override
    			public void open() throws HyracksDataException {
    				state = (BuildAndPartitionTaskState) env.getTaskState(new TaskId(new ActivityId(getOperatorId(),
                            BUILD_AND_PARTITION_ACTIVITY_ID), partition));
    				
    				writer.open();
    				state.hybridHJ.initProbe();
    				
    			}

    			@Override
    			public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
    				state.hybridHJ.probe(buffer, writer);
    			}

    			@Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

    			@Override
    			public void close() throws HyracksDataException {

    				state.hybridHJ.closeProbe(writer);
    				
    				BitSet partitionStatus = state.hybridHJ.getPartitinStatus();
    				hpcRep0 = new RepartitionComputerGeneratorFactory(state.numOfPartitions, hpcf0).createPartitioner(0);
    				hpcRep1 = new RepartitionComputerGeneratorFactory(state.numOfPartitions, hpcf1).createPartitioner(0);
    				
    				rPartbuff.clear();     
    				for(int pid=partitionStatus.nextSetBit(0); pid>=0; pid=partitionStatus.nextSetBit(pid+1)){

    					RunFileReader bReader = state.hybridHJ.getBuildRFReader(pid);
						RunFileReader pReader = state.hybridHJ.getProbeRFReader(pid);
    						
    					if (bReader == null || pReader == null) {
    						continue;
    					}
    					int bSize = state.hybridHJ.getBuildPartitionSizeInTup(pid);
    					int pSize = state.hybridHJ.getProbePartitionSizeInTup(pid);
    					int beforeMax = (bSize>pSize) ? bSize : pSize;
    					joinPartitionPair(state.hybridHJ, bReader, pReader, pid, beforeMax, 1);
    						
    				}
    				writer.close();
    			}
    			
    			private void joinPartitionPair(OptimizedHybridHashJoin ohhj, RunFileReader buildSideReader, RunFileReader probeSideReader, int pid, int beforeMax, int level) throws HyracksDataException{
    				ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerGeneratorFactory(probeKeys, hashFunctionGeneratorFactories).createPartitioner(level);
                	ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerGeneratorFactory(buildKeys, hashFunctionGeneratorFactories).createPartitioner(level);
        			
    				if(level > maxLevel){
    					maxLevel = level;
    				}
    				
    				long buildPartSize = ohhj.getBuildPartitionSize(pid) / ctx.getFrameSize();
					long probePartSize = ohhj.getProbePartitionSize(pid) / ctx.getFrameSize();
    				
						//Apply in-Mem HJ if possible
					if( (buildPartSize<state.memForJoin) || (probePartSize<state.memForJoin)  ){
						int tabSize = -1;
						if(buildPartSize<probePartSize){
							tabSize = ohhj.getBuildPartitionSizeInTup(pid);
							if(tabSize>0){	//Build Side is smaller
								applyInMemHashJoin(probeKeys, buildKeys, tabSize, probeRd, buildRd, hpcRep1, hpcRep0, buildSideReader, probeSideReader);
							}
						}
						else{   //Role Reversal
							tabSize = ohhj.getProbePartitionSizeInTup(pid);
							if(tabSize>0){	//Probe Side is smaller
								applyInMemHashJoin(buildKeys, probeKeys, tabSize, buildRd, probeRd, hpcRep0, hpcRep1, probeSideReader, buildSideReader);
							}
						}
					}
						//Apply (Recursive) HHJ
					else {
						OptimizedHybridHashJoin rHHj;
						if(buildPartSize<probePartSize){	//Build Side is smaller
							
							int n = getNumberOfPartitions(state.memForJoin, (int) buildPartSize, factor, nPartitions);
							rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin,
									n, PROBE_REL, BUILD_REL,
										probeKeys, buildKeys, comparators,
											probeRd, buildRd, probeHpc, buildHpc);
							
							
							buildSideReader.open();
							rHHj.initBuild();
		    				rPartbuff.clear();
		    				while (buildSideReader.nextFrame(rPartbuff)) {
		    					rHHj.build(rPartbuff);
		    				}
		    				
		    				rHHj.closeBuild();
		    				
		    				probeSideReader.open();
		    				rHHj.initProbe();
		    				rPartbuff.clear();
		    				while (probeSideReader.nextFrame(rPartbuff)) {
		    					rHHj.probe(rPartbuff, writer);
		    				}
		    				rHHj.closeProbe(writer);
		    				
		    				int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
		    				int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
		    				int afterMax = (maxAfterBuildSize>maxAfterProbeSize) ? maxAfterBuildSize : maxAfterProbeSize;
		    				
		    				BitSet rPStatus = rHHj.getPartitinStatus();
		    				if(afterMax < NLJ_SWITCH_THRESHOLD*beforeMax){
		    					for(int rPid=rPStatus.nextSetBit(0); rPid>=0; rPid=rPStatus.nextSetBit(rPid+1)){
		    						RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
		    						RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
		    						
		    						if (rbrfw == null || rprfw == null) {
		        						continue;
		        					}
		    						
		    						joinPartitionPair(rHHj, rbrfw, rprfw, rPid, afterMax, (level+1));
		    					}
		    					
		    				}
		    				else{	//Switch to NLJ (Further recursion seems not to be useful)
		    					for(int rPid=rPStatus.nextSetBit(0); rPid>=0; rPid=rPStatus.nextSetBit(rPid+1)){
		    						RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
		    						RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
		    						
		    						if (rbrfw == null || rprfw == null) {
		        						continue;
		        					}
		    						
		    						int buildSideInTups = rHHj.getBuildPartitionSizeInTup(rPid);
		    						int probeSideInTups = rHHj.getProbePartitionSizeInTup(rPid);
		    						if(buildSideInTups<probeSideInTups){
		    							applyNestedLoopJoin(probeRd, buildRd, state.memForJoin, rbrfw, rprfw, nljComparator0);
		    						}
		    						else{
		    							applyNestedLoopJoin(buildRd, probeRd, state.memForJoin, rprfw, rbrfw, nljComparator1);
		    						}
		    					}
		    				}
						}
						else{ //Role Reversal (Probe Side is smaller)
							int n = getNumberOfPartitions(state.memForJoin, (int) probePartSize, factor, nPartitions);
							rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin,
									n, BUILD_REL, PROBE_REL,
										buildKeys, probeKeys, comparators,
											buildRd, probeRd,buildHpc, probeHpc);
							
							probeSideReader.open();
							rHHj.initBuild();
		    				rPartbuff.clear();
		    				while (probeSideReader.nextFrame(rPartbuff)) {
		    					rHHj.build(rPartbuff);
		    				}
		    				rHHj.closeBuild();
		    				rHHj.initProbe();
		    				buildSideReader.open();
		    				rPartbuff.clear();
		    				while (buildSideReader.nextFrame(rPartbuff)) {
		    					rHHj.probe(rPartbuff, writer);
		    				}
		    				rHHj.closeProbe(writer);
		    				int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
		    				int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
		    				int afterMax = (maxAfterBuildSize>maxAfterProbeSize) ? maxAfterBuildSize : maxAfterProbeSize;
		    				BitSet rPStatus = rHHj.getPartitinStatus();
		    				
		    				if(afterMax < NLJ_SWITCH_THRESHOLD*beforeMax){
		    					for(int rPid=rPStatus.nextSetBit(0); rPid>=0; rPid=rPStatus.nextSetBit(rPid+1)){
		    						RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
		    						RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
		    						
		    						if (rbrfw == null || rprfw == null) {
		        						continue;
		        					}
		    						
		    						joinPartitionPair(rHHj, rprfw, rbrfw, rPid, afterMax, (level+1));
		    					}
		    				}
		    				else{	//Switch to NLJ (Further recursion seems not to be effective)
		    					for(int rPid=rPStatus.nextSetBit(0); rPid>=0; rPid=rPStatus.nextSetBit(rPid+1)){
		    						RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
		    						RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
		    						
		    						if (rbrfw == null || rprfw == null) {
		        						continue;
		        					}
		    						
		    						long buildSideSize = rbrfw.getFileSize();
		    						long probeSideSize = rprfw.getFileSize();
		    						if(buildSideSize>probeSideSize){
		    							applyNestedLoopJoin(buildRd, probeRd, state.memForJoin, rbrfw, rprfw, nljComparator1);
		    						}
		    						else{
		    							applyNestedLoopJoin(probeRd, buildRd, state.memForJoin, rprfw, rbrfw, nljComparator0);
		    						}
		    					}
		    				}
						}
						buildSideReader.close();
						probeSideReader.close();
					}
    			}
    			
    			private void applyInMemHashJoin(int[] bKeys, int[] pKeys, int tabSize, 
    					RecordDescriptor smallerRd, RecordDescriptor largerRd, 
    					ITuplePartitionComputer hpcRepLarger, ITuplePartitionComputer hpcRepSmaller,
    					RunFileReader bReader, RunFileReader pReader) throws HyracksDataException{
    				
    				ISerializableTable table = new SerializableHashTable(tabSize, ctx);
    				InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tabSize, 
    						new FrameTupleAccessor(ctx.getFrameSize(), largerRd), hpcRepLarger, 
    						new FrameTupleAccessor(ctx.getFrameSize(), smallerRd), hpcRepSmaller, 
    						new FrameTuplePairComparator(pKeys, bKeys, comparators), isLeftOuter, nullWriters1, table);

    				bReader.open();
    				rPartbuff.clear();
    				while (bReader.nextFrame(rPartbuff)) {
    					ByteBuffer copyBuffer = ctx.allocateFrame();
    					FrameUtils.copy(rPartbuff, copyBuffer);
    					FrameUtils.makeReadable(copyBuffer);
    					joiner.build(copyBuffer);
    					rPartbuff.clear();
    				}
    				bReader.close();
    				rPartbuff.clear();
    				// probe
    				pReader.open();
    				while (pReader.nextFrame(rPartbuff)) {
    					joiner.join(rPartbuff, writer);
    					rPartbuff.clear();
    				}
    				pReader.close();
    				joiner.closeJoin(writer);
    			}

    			private void applyNestedLoopJoin(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize, RunFileReader outerReader, RunFileReader innerReader, ITuplePairComparator nljCompaarator) throws HyracksDataException{
    				
    				NestedLoopJoin nlj = new NestedLoopJoin(ctx, new FrameTupleAccessor(ctx.getFrameSize(), outerRd),
    						new FrameTupleAccessor(ctx.getFrameSize(), innerRd), 
    						nljCompaarator, memorySize);


    				ByteBuffer cacheBuff = ctx.allocateFrame();
    				innerReader.open();
    				while(innerReader.nextFrame(cacheBuff)){
    					FrameUtils.makeReadable(cacheBuff);
    					nlj.cache(cacheBuff);
    					cacheBuff.clear();
    				}
    				nlj.closeCache();
    	
    				ByteBuffer joinBuff = ctx.allocateFrame();
    				outerReader.open();
    				
    				while(outerReader.nextFrame(joinBuff)){
    					FrameUtils.makeReadable(joinBuff);
    					nlj.join(joinBuff, writer);
    					joinBuff.clear();
    				}
    				
    				nlj.closeJoin(writer);
    				outerReader.close();
    				innerReader.close();
    			}
    		};
    		return op;
    	}
    }
}
