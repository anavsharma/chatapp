package com.cmpe275.server;

import com.cmpe275.util.Connection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * copyright 2018, gash
 *
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class EdgeServer {
    protected static Logger LOG = LoggerFactory.getLogger(EdgeServer.class.getName());
    protected static AtomicReference<EdgeServer> instance = new AtomicReference<EdgeServer>();
    protected static Config conf;
    public static String serverConfigString;
    public static List<Connection> localServerList;
    public static List<Connection> globalServerList;
    public static List<Connection> coordinationServerList;

    // host details
    protected static Integer hostPort;
    protected static Long hostID;
    protected static String hostIP;

    protected Long nextMessageID;

    private EdgeServer(){
        init();
    }

    public static void configure(String configArg){
        EdgeServer.conf = ConfigFactory.load();
        EdgeServer.serverConfigString = configArg;
    }

    public static EdgeServer getInstance(){
        instance.compareAndSet(null, new EdgeServer());
        return instance.get();
    }

    private List<Connection> initLocalServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_1"),
                            conf.getInt(this.serverConfigString+".localServerList.svrPort_1"));
        Connection svr_2 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_2"),
                            conf.getInt(this.serverConfigString+".localServerList.svrPort_2"));
        Connection svr_3 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_3"),
                            conf.getInt(this.serverConfigString+".localServerList.svrPort_3"));

        local_svr_list.add(svr_1);
        local_svr_list.add(svr_2);
        local_svr_list.add(svr_3);
        return local_svr_list;
    }

    private List<Connection> initGlobalServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_1"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrPort_1"));
        Connection svr_2 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_2"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrPort_2"));
        Connection svr_3 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_3"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrPort_3"));

        local_svr_list.add(svr_1);
        local_svr_list.add(svr_2);
        local_svr_list.add(svr_3);
        return local_svr_list;
    }

    private List<Connection> initCoordiantionServerList(Config conf){
        List<Connection> coordinationServerList = new ArrayList<Connection>();
        Connection svr_1 = new Connection(
                conf.getString(this.serverConfigString+".coordinationServerList.svrIP_1"),
                conf.getInt(this.serverConfigString+".coordinationServerList.svrPort_1"));
        Connection svr_2 = new Connection(
                conf.getString(this.serverConfigString+".coordinationServerList.svrIP_2"),
                conf.getInt(this.serverConfigString+".coordinationServerList.svrPort_2"));
        Connection svr_3 = new Connection(
                conf.getString(this.serverConfigString+".coordinationServerList.svrIP_3"),
                conf.getInt(this.serverConfigString+".coordinationServerList.svrPort_3"));

        coordinationServerList.add(svr_1);
        coordinationServerList.add(svr_2);
        coordinationServerList.add(svr_3);
        return coordinationServerList;

    }

    private void init() {
        if (conf == null) {
            throw new RuntimeException("server not configured!");
        }

        String tmp_ip = conf.getString(this.serverConfigString+".serverConig.hostIP");
        if (tmp_ip == null)
            throw new RuntimeException("missing server ID");
        hostIP = tmp_ip;


        Long tmp_id = conf.getLong(this.serverConfigString+".serverConfig.hostID");
        if (tmp_id == null)
            throw new RuntimeException("missing server ID");
        hostID = tmp_id;

        Integer tmp_port = conf.getInt(this.serverConfigString+".serverConfig.hostPort");
        if (tmp_port == null)
            throw new RuntimeException("missing server port");
        hostPort = tmp_port;

        if (hostPort <= 1024)
            throw new RuntimeException("server port must be above 1024");

        localServerList = initLocalServerList(conf);
        globalServerList = initGlobalServerList(conf);
        coordinationServerList = initCoordiantionServerList(conf);

        nextMessageID = 0L;
    }

    public static Config getConf() {
        return conf;
    }

    public Long getServerID() {
        return hostID;
    }

    public synchronized Long getNextMessageID() {
        return ++nextMessageID;
    }

    public static Integer getServerPort() {
        return hostPort;
    }

    public static String getServerIP(){
        return hostIP;
    }

    public static List<Connection> getLocalServerList() {
        return localServerList;
    }
}
