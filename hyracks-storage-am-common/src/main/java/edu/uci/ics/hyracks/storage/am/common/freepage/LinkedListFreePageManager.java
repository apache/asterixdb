package edu.uci.ics.hyracks.storage.am.common.freepage;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class LinkedListFreePageManager implements IFreePageManager {

	private final IBufferCache bufferCache;
	private final int fileId;
	private final int headPage;
	private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;

	public LinkedListFreePageManager(IBufferCache bufferCache, int fileId, int headPage, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
		this.bufferCache = bufferCache;
		this.fileId = fileId;
		this.headPage = headPage;
		this.metaDataFrameFactory = metaDataFrameFactory;
	}

	@Override
	public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage)
			throws HyracksDataException {

		ICachedPage metaNode = bufferCache.pin(BufferedFileHandle
				.getDiskPageId(fileId, headPage), false);
		metaNode.acquireWriteLatch();

		try {
			metaFrame.setPage(metaNode);

			if (metaFrame.hasSpace()) {
				metaFrame.addFreePage(freePage);
			} else {
				// allocate a new page in the chain of meta pages
				int newPage = metaFrame.getFreePage();
				if (newPage < 0) {
					throw new Exception(
							"Inconsistent Meta Page State. It has no space, but it also has no entries.");
				}

				ICachedPage newNode = bufferCache.pin(BufferedFileHandle
						.getDiskPageId(fileId, newPage), false);
				newNode.acquireWriteLatch();

				try {
					int metaMaxPage = metaFrame.getMaxPage();

					// copy metaDataPage to newNode
					System.arraycopy(metaNode.getBuffer().array(), 0, newNode
							.getBuffer().array(), 0, metaNode.getBuffer()
							.capacity());

					metaFrame.initBuffer(-1);
					metaFrame.setNextPage(newPage);
					metaFrame.setMaxPage(metaMaxPage);
					metaFrame.addFreePage(freePage);
				} finally {
					newNode.releaseWriteLatch();
					bufferCache.unpin(newNode);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			metaNode.releaseWriteLatch();
			bufferCache.unpin(metaNode);
		}
	}

	@Override
	public int getFreePage(ITreeIndexMetaDataFrame metaFrame)
			throws HyracksDataException {
		ICachedPage metaNode = bufferCache.pin(BufferedFileHandle
				.getDiskPageId(fileId, headPage), false);

		metaNode.acquireWriteLatch();

		int freePage = -1;
		try {
			metaFrame.setPage(metaNode);
			freePage = metaFrame.getFreePage();
			if (freePage < 0) { // no free page entry on this page
				int nextPage = metaFrame.getNextPage();
				if (nextPage > 0) { // sibling may have free pages
					ICachedPage nextNode = bufferCache.pin(BufferedFileHandle
							.getDiskPageId(fileId, nextPage), false);

					nextNode.acquireWriteLatch();
					// we copy over the free space entries of nextpage into the
					// first meta page (metaDataPage)
					// we need to link the first page properly to the next page
					// of nextpage
					try {
						// remember entries that remain unchanged
						int maxPage = metaFrame.getMaxPage();

						// copy entire page (including sibling pointer, free
						// page entries, and all other info)
						// after this copy nextPage is considered a free page
						System.arraycopy(nextNode.getBuffer().array(), 0,
								metaNode.getBuffer().array(), 0, nextNode
										.getBuffer().capacity());

						// reset unchanged entry
						metaFrame.setMaxPage(maxPage);

						freePage = metaFrame.getFreePage();
						// sibling also has no free pages, this "should" not
						// happen, but we deal with it anyway just to be safe
						if (freePage < 0) {
							freePage = nextPage;
						} else {
							metaFrame.addFreePage(nextPage);
						}
					} finally {
						nextNode.releaseWriteLatch();
						bufferCache.unpin(nextNode);
					}
				} else {
					freePage = metaFrame.getMaxPage();
					freePage++;
					metaFrame.setMaxPage(freePage);
				}
			}
		} finally {
			metaNode.releaseWriteLatch();
			bufferCache.unpin(metaNode);
		}

		return freePage;
	}

	@Override
	public int getMaxPage(ITreeIndexMetaDataFrame metaFrame)
			throws HyracksDataException {
		ICachedPage metaNode = bufferCache.pin(BufferedFileHandle
				.getDiskPageId(fileId, headPage), false);
		metaNode.acquireWriteLatch();
		int maxPage = -1;
		try {
			metaFrame.setPage(metaNode);
			maxPage = metaFrame.getMaxPage();
		} finally {
			metaNode.releaseWriteLatch();
			bufferCache.unpin(metaNode);
		}
		return maxPage;
	}

	@Override
	public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage)
			throws HyracksDataException {
		// initialize meta data page
		ICachedPage metaNode = bufferCache.pin(BufferedFileHandle
				.getDiskPageId(fileId, headPage), false);

		metaNode.acquireWriteLatch();
		try {
			metaFrame.setPage(metaNode);
			metaFrame.initBuffer((byte) -1);
			metaFrame.setMaxPage(currentMaxPage);
		} finally {
			metaNode.releaseWriteLatch();
			bufferCache.unpin(metaNode);
		}
	}

	@Override
	public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory() {
		return metaDataFrameFactory;
	}
}
