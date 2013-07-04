package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class MaterializingFrameWriter implements IFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(MaterializingFrameWriter.class.getName());

    private IFrameWriter writer;

    private List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

    private Mode mode;

    public enum Mode {
        FORWARD,
        STORE
    }

    public MaterializingFrameWriter(IFrameWriter writer) {
        this.writer = writer;
        this.mode = Mode.FORWARD;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode newMode) throws HyracksDataException {
        if (this.mode.equals(newMode)) {
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to :" + newMode + " from " + this.mode);
        }
        switch (newMode) {
            case FORWARD:
                this.mode = newMode;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Sending accumulated frames :" + frames.size());
                }
                break;
            case STORE:
                this.mode = newMode;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning to store frames :");
                    LOGGER.info("Frames accumulated till now:" + frames.size());
                }
                break;
        }

    }

    public List<ByteBuffer> getStoredFrames() {
        return frames;
    }

    public void clear() {
        frames.clear();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        switch (mode) {
            case FORWARD:
                writer.nextFrame(buffer);
                if (frames.size() > 0) {
                    for (ByteBuffer buf : frames) {
                        System.out.println("Flusing OLD frame: " + buf);
                        writer.nextFrame(buf);
                    }
                }
                frames.clear();
                break;
            case STORE:
                ByteBuffer storageBuffer = ByteBuffer.allocate(buffer.capacity());
                storageBuffer.put(buffer);
                frames.add(storageBuffer);
                storageBuffer.flip();
                break;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    public IFrameWriter getWriter() {
        return writer;
    }

    public void setWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "MaterializingFrameWriter using " + writer;
    }
}
