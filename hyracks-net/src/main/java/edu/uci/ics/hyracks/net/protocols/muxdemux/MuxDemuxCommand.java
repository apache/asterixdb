package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.nio.ByteBuffer;

class MuxDemuxCommand {
    static final int COMMAND_SIZE = 4;

    enum CommandType {
        OPEN_CHANNEL,
        CLOSE_CHANNEL,
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

    public void setChannelId(int channelId) {
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

    public void setData(int data) {
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