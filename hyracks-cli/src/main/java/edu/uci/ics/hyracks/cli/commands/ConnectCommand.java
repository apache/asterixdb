package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.cli.Session;

public class ConnectCommand extends Command {
    private String host;

    private int port;

    public ConnectCommand(String hostPortStr) {
        int idx = hostPortStr.indexOf(':');
        host = hostPortStr;
        port = 1098;
        if (idx != -1) {
            host = hostPortStr.substring(0, idx);
            port = Integer.valueOf(hostPortStr.substring(idx + 1));
        }
    }

    @Override
    public void run(Session session) throws Exception {
        System.err.println("Connecting to host: " + host + ", port: " + port);
        IHyracksClientConnection conn = new HyracksConnection(host, port);
        session.setConnection(conn);
    }
}