package edu.uci.ics.asterix.indexing.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameMeta;
import edu.uci.ics.asterix.storage.buffercache.ICachedPage;

// all meta pages of this kind have a negative level
// the first meta page has level -1, all other meta pages have level -2
// the first meta page is special because it guarantees to have a correct max page
// other meta pages (i.e., with level -2) have junk in the max page field

public class BTreeMeta implements IBTreeFrameMeta {
        
    protected static final int numRecordsOff = 0;             
    protected static final int freeSpaceOff = numRecordsOff + 4;
    protected static final int maxPageOff = freeSpaceOff + 4;
    protected static final byte levelOff = maxPageOff + 1;
    protected static final byte nextPageOff = maxPageOff + 8;
    
    protected ICachedPage page = null;
    protected ByteBuffer buf = null;    
    
    public int getMaxPage() {
        return buf.getInt(maxPageOff);
    }
    
    public void setMaxPage(int maxPage) {
        buf.putInt(maxPageOff, maxPage);
    }
        
    public int getFreePage() {                
        int numRecords = buf.getInt(numRecordsOff); 
        if(numRecords > 0) {
            // return the last page from the linked list of free pages
            // TODO: this is a dumb policy, but good enough for now
            int lastPageOff = buf.getInt(freeSpaceOff) - 4;
            buf.putInt(freeSpaceOff, lastPageOff);
            buf.putInt(numRecordsOff, numRecords - 1);
            return buf.getInt(lastPageOff);
        }
        else {
            return -1;
        }                                    
    }
    
    
    // must be checked before adding free page
    // user of this class is responsible for getting a free page as a new meta page, latching it, etc. if there is no space on this page
    public boolean hasSpace() {
        return buf.getInt(freeSpaceOff) + 4 < buf.capacity();
    }
    
    // on bounds checking is done, there must be free space
    public void addFreePage(int freePage) {
        int freeSpace = buf.getInt(freeSpaceOff);
        buf.putInt(freeSpace, freePage);
        buf.putInt(freeSpaceOff, freeSpace + 4);
        buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) + 1);
    }

    @Override
    public byte getLevel() {        
        return buf.get(levelOff);
    }
    
    @Override
    public void setLevel(byte level) {
        buf.put(levelOff, level);           
    }
    
    @Override
    public ICachedPage getPage() {
        return page;
    }   

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();        
    }

    @Override
    public void initBuffer(int level) {
        buf.putInt(freeSpaceOff, nextPageOff + 4);
        buf.putInt(numRecordsOff, 0);
        buf.putInt(levelOff, level);
        buf.putInt(nextPageOff, -1);
    }
    
    @Override
    public int getNextPage() {
        return buf.getInt(nextPageOff);
    }

    @Override
    public void setNextPage(int nextPage) {
        buf.putInt(nextPageOff, nextPage);
    }        
}
