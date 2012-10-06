package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.net.exceptions.NetException;

class MuxDemuxCommand {
    static final int MAX_CHANNEL_ID = Integer.MAX_VALUE - 1;

    static final int COMMAND_SIZE = 8;

    static final int MAX_DATA_VALUE = 0x1fffffff;

    enum CommandType {
        OPEN_CHANNEL,
        CLOSE_CHANNEL,
        CLOSE_CHANNEL_ACK,
        ERROR,
        ADD_CREDITS,
        DATA,
    }

    private int channelId;

    private CommandType type;

    private int data;

    public int getChannelId() {
        return channelId;
    }

    public void setChannelId(int channelId) throws NetException {
        if (channelId > MAX_CHANNEL_ID) {
            throw new NetException("channelId " + channelId + " exceeds " + MAX_CHANNEL_ID);
        }
        this.channelId = channelId;
    }

    public CommandType getCommandType() {
        return type;
    }

    public void setCommandType(CommandType type) {
        this.type = type;
    }

    public int getData() {
        return data;
    }

    public void setData(int data) throws NetException {
        if (data > MAX_DATA_VALUE) {
            throw new NetException("data " + data + " exceeds " + MAX_DATA_VALUE);
        }
        this.data = data;
    }

    public void write(ByteBuffer buffer) {
        long cmd = (((long) channelId) << 32) | (((long) type.ordinal()) << 29) | (data & 0x1fffffff);
        buffer.putLong(cmd);
    }

    public void read(ByteBuffer buffer) {
        long cmd = buffer.getLong();
        channelId = (int) ((cmd >> 32) & 0x7fffffff);
        type = CommandType.values()[(int) ((cmd >> 29) & 0x7)];
        data = (int) (cmd & 0x1fffffff);
    }

    @Override
    public String toString() {
        return channelId + ":" + type + ":" + data;
    }
}