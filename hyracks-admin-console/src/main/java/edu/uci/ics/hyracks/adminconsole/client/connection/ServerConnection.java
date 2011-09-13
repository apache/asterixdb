package edu.uci.ics.hyracks.adminconsole.client.connection;

public class ServerConnection {
    private String serverURLPrefix;

    public ServerConnection() {
    }

    public String getServerURLPrefix() {
        return serverURLPrefix;
    }

    public void setServerURLPrefix(String serverURLPrefix) {
        this.serverURLPrefix = serverURLPrefix;
    }
}