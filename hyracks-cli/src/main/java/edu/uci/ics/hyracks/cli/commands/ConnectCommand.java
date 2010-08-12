package edu.uci.ics.hyracks.cli.commands;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.cli.CLI;

public class ConnectCommand extends Command {
    private String host;

    private int port;

    public ConnectCommand(String hostPortStr) {
        int idx = hostPortStr.indexOf(':');
        host = hostPortStr;
        port = 1099;
        if (idx != -1) {
            host = hostPortStr.substring(0, idx);
            port = Integer.valueOf(hostPortStr.substring(idx + 1));
        }
    }

    @Override
    public void run(CLI cli) throws Exception {
        System.err.println("Connecting to host: " + host + ", port: " + port);
        IHyracksClientConnection conn = new HyracksRMIConnection(host, port);
        cli.setConnection(conn);
    }
}