package com.cmpe275.server;

public class Connection {
    public final String ipAddress;
    public final Long port;
    public final Integer id;

    public Connection(String ipAddr, Long port, Integer id){
        this.ipAddress = ipAddr;
        this.port = port;
        this.id = id;
    }

}
