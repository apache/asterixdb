package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public interface IBTreeFrameMeta {
    public void initBuffer(int level);
    
    public void setPage(ICachedPage page);
    public ICachedPage getPage();
    
    public byte getLevel();
    public void setLevel(byte level);
    
    public int getNextPage();
    public void setNextPage(int nextPage);
    
    public int getMaxPage();
    public void setMaxPage(int maxPage);
    
    public int getFreePage();
    public boolean hasSpace();
    public void addFreePage(int freePage);
}
