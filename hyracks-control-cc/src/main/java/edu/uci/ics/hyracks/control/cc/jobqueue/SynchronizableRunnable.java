package edu.uci.ics.hyracks.control.cc.jobqueue;

public abstract class SynchronizableRunnable implements Runnable {
    private boolean done;

    private Exception e;

    protected abstract void doRun() throws Exception;

    public void init() {
        done = false;
        e = null;
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (Exception e) {
            this.e = e;
        } finally {
            synchronized (this) {
                done = true;
                notifyAll();
            }
        }
    }

    public final synchronized void sync() throws Exception {
        while (!done) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (e != null) {
            throw e;
        }
    }
}