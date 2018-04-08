package com.wplay.hadoop.util;

import org.apache.hadoop.ipc.Server;
import org.apache.log4j.Logger;

public class ServerThread extends Thread {
    private final static Logger LOG = Logger.getLogger(ServerThread.class);
    private final Server server;

    public ServerThread(Server server) {
	this.server = server;
    }

    @Override
    public void run() {
	try {
	    server.start();
	    server.join();
	} catch (Exception e) {
	    LOG.error(e.getMessage(), e);
	}
    }
}