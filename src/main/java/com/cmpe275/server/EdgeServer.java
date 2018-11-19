package com.cmpe275.server;

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

public class CliqueServer {
    protected static Logger LOG = LoggerFactory.getLogger("server");
    protected static AtomicReference<CliqueServer> instance = new AtomicReference<CliqueServer>();
    protected static Config conf;
    protected static Boolean isGlobalServer;
    protected static String serverConfigString;
    protected static List<Connection> localServerList;
    protected static List<Connection> globalServerList;

    // host details
    protected static Integer hostPort;
    protected static Long hostID;
    protected static String hostIP;

    protected Long nextMessageID;

    private CliqueServer(){
        init();
    }

    public static void configure(String configArg){
        CliqueServer.conf = ConfigFactory.load();
        CliqueServer.serverConfigString = configArg;
    }

    public static CliqueServer getInstance(){
        instance.compareAndSet(null, new CliqueServer());
        return instance.get();
    }

    private List<Connection> initLocalServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_1"),
                            conf.getLong(this.serverConfigString+".localServerList.svrPort_1"),
                            conf.getInt(this.serverConfigString+".localServerList.svrID_1"));
        Connection svr_2 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_2"),
                            conf.getLong(this.serverConfigString+".localServerList.svrPort_2"),
                            conf.getInt(this.serverConfigString+".localServerList.svrID_2"));
        Connection svr_3 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_3"),
                            conf.getLong(this.serverConfigString+".localServerList.svrPort_3"),
                            conf.getInt(this.serverConfigString+".localServerList.svrID_3") );

        local_svr_list.add(svr_1);
        local_svr_list.add(svr_2);
        local_svr_list.add(svr_3);
        return local_svr_list;
    }

    private List<Connection> initGlobalServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_1"),
                            conf.getLong(this.serverConfigString+".globalServerList.svrPort_1"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrID_1"));
        Connection svr_2 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_2"),
                            conf.getLong(this.serverConfigString+".globalServerList.svrPort_2"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrID_2"));
        Connection svr_3 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_3"),
                            conf.getLong(this.serverConfigString+".globalServerList.svrPort_3"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrID_3"));

        local_svr_list.add(svr_1);
        local_svr_list.add(svr_2);
        local_svr_list.add(svr_3);
        return local_svr_list;
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
        isGlobalServer = conf.getBoolean(this.serverConfigString+".isGlobalServer");

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
