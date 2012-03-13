package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.net.exceptions.NetException;

class MuxDemuxCommand {
    static final int MAX_CHANNEL_ID = 0x3ff;

    static final int COMMAND_SIZE = 4;

    static final int MAX_DATA_VALUE = 0x7ffff;

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
        int cmd = (channelId << 22) | (type.ordinal() << 19) | (data & 0x7ffff);
        buffer.putInt(cmd);
    }

    public void read(ByteBuffer buffer) {
        int cmd = buffer.getInt();
        channelId = (cmd >> 22) & 0x3ff;
        type = CommandType.values()[(cmd >> 19) & 0x7];
        data = cmd & 0x7ffff;
    }

    @Override
    public String toString() {
        return channelId + ":" + type + ":" + data;
    }
}