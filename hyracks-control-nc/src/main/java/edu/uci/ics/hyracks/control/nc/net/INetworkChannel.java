package edu.uci.ics.hyracks.control.nc.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface INetworkChannel {
    public boolean dispatchNetworkEvent() throws IOException;

    public void setSelectionKey(SelectionKey key);

    public void abort();

    public void notifyConnectionManagerRegistration() throws IOException;
}