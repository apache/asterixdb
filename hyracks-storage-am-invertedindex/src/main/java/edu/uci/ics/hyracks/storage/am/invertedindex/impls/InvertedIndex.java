package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class InvertedIndex {
	private int rootPageId = 0;
	private IBufferCache bufferCache;
    private int fileId;
		
	public BulkLoadContext beginBulkLoad(IInvertedListBuilder invListBuilder, IBinaryComparator tokenCmp) throws HyracksDataException {
		BulkLoadContext ctx = new BulkLoadContext(invListBuilder, tokenCmp);
		ctx.init(rootPageId, fileId);
		return ctx;
	}
	
	public void bulkLoadAddTuple(BulkLoadContext ctx, ITupleReference tuple, int tokenField, int[] listElementFields) throws HyracksDataException {
				
		// first inverted list, copy token to baaos and start new list
		if(ctx.currentInvListTokenBaaos.size() == 0) {
			ctx.currentInvListStartPageId = ctx.currentPageId;
			ctx.currentInvListStartOffset = ctx.invListBuilder.getPos();
			
			ctx.currentInvListTokenBaaos.reset();
			ctx.currentInvListTokenBaaos.write(tuple.getFieldData(tokenField), tuple.getFieldStart(tokenField), tuple.getFieldLength(tokenField));
			
			if(!ctx.invListBuilder.startNewList(tuple, tokenField)) {
				ctx.pinNextPage();
				ctx.invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
				if(!ctx.invListBuilder.startNewList(tuple, tokenField)) {
					throw new IllegalStateException("Failed to create first inverted list.");
				}								
			}
		}
		
		// create new inverted list?
		if(ctx.tokenCmp.compare(tuple.getFieldData(tokenField), 
				tuple.getFieldStart(tokenField), 
				tuple.getFieldLength(tokenField), 
				ctx.currentInvListTokenBaaos.getByteArray(), 
				0, 
				ctx.currentInvListTokenBaaos.size()) != 0) {

			ctx.lastInvListStartPageId = ctx.currentInvListStartPageId;
			ctx.lastInvListStartOffset = ctx.currentInvListStartOffset;
			
			ctx.lastInvListTokenBaaos.reset();
			ctx.lastInvListTokenBaaos.write(ctx.currentInvListTokenBaaos.getByteArray(), 0, ctx.currentInvListTokenBaaos.size());
			
			ctx.currentInvListTokenBaaos.reset();
			ctx.currentInvListTokenBaaos.write(tuple.getFieldData(tokenField), tuple.getFieldStart(tokenField), tuple.getFieldLength(tokenField));

			ctx.lastInvListSize = ctx.invListBuilder.getListSize();
			if(!ctx.invListBuilder.startNewList(tuple, tokenField)) {
				ctx.pinNextPage();
				ctx.invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
				if(!ctx.invListBuilder.startNewList(tuple, tokenField)) {
					throw new IllegalStateException("Failed to start new inverted list after switching to a new page.");
				}								
			}
			
			ctx.currentInvListStartPageId = ctx.currentPageId;
			ctx.currentInvListStartOffset = ctx.invListBuilder.getPos();
		}

		// append to current inverted list
		if(!ctx.invListBuilder.appendElement(tuple, listElementFields)) {
			ctx.pinNextPage();
			ctx.invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
			if(!ctx.invListBuilder.appendElement(tuple, listElementFields)) {
				throw new IllegalStateException("Failed to append element to inverted list after switching to a new page.");
			}								
		}
	}
	
	// returns size of last inverted list
	public int endBulkLoad(BulkLoadContext ctx) throws HyracksDataException {		
		ctx.lastInvListStartPageId = ctx.currentInvListStartPageId;
		ctx.lastInvListStartOffset = ctx.currentInvListStartOffset;
		
		ctx.lastInvListTokenBaaos.reset();
		ctx.lastInvListTokenBaaos.write(ctx.currentInvListTokenBaaos.getByteArray(), 0, ctx.currentInvListTokenBaaos.size());
		
		ctx.deinit();
		return ctx.invListBuilder.getListSize();
	}
	
	public final class BulkLoadContext {		
		private int lastInvListSize;
		private int lastInvListStartPageId;
		private int lastInvListStartOffset;
		private final ByteArrayAccessibleOutputStream lastInvListTokenBaaos = new ByteArrayAccessibleOutputStream();
		
		private int currentInvListStartPageId;
		private int currentInvListStartOffset;					
		private final ByteArrayAccessibleOutputStream currentInvListTokenBaaos = new ByteArrayAccessibleOutputStream();
		
		private int currentPageId;
		private ICachedPage currentPage;		
		private final IInvertedListBuilder invListBuilder;	
		private final IBinaryComparator tokenCmp;
		
		public BulkLoadContext(IInvertedListBuilder invListBuilder, IBinaryComparator tokenCmp) {
			this.invListBuilder = invListBuilder;
			this.tokenCmp = tokenCmp;			
		}
		
		public void init(int startPageId, int fileId) throws HyracksDataException {
			currentPageId = startPageId;
			currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
			invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
		}
		
		public void deinit() throws HyracksDataException {
			if(currentPage != null) bufferCache.unpin(currentPage);
		}
		
		public void pinNextPage() throws HyracksDataException {
			bufferCache.unpin(currentPage);
			currentPageId++;
			currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
		}
		
		public ByteArrayAccessibleOutputStream getLastInvListTokenBaaos() {
			return lastInvListTokenBaaos;
		}
		
		public int getLastInvListStartPageId() {
			return lastInvListStartPageId;
		}
		
		public int getLastInvListStartOffset() {
			return lastInvListStartOffset;
		}
		
		public int getLastInvListSize() {
			return lastInvListSize;
		}
	};	
}
