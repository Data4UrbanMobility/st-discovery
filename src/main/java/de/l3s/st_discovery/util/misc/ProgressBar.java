package de.l3s.st_discovery.util.misc;

import me.tongfei.progressbar.ProgressBarStyle;

public class ProgressBar {

    private me.tongfei.progressbar.ProgressBar pb;
    private boolean running;
    private boolean stopped;

    public ProgressBar(String task, int workload) {
        pb = new me.tongfei.progressbar.ProgressBar(task, workload, ProgressBarStyle.ASCII);
        running=false;
        stopped=false;
    }

    public void start() {
        if (!running) pb.start();
        running=true;
    }

    public synchronized void step() {
        pb.step();
        if (pb.getCurrent() == pb.getMax()) stop();
    }

    public synchronized void stop() {
        if ((!running) || stopped) return;

        pb.stop();
        running=false;
        stopped=true;
    }
}