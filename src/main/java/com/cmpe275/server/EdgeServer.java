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
    public static List<Connection> globalServerList_t1;
    public static List<Connection> globalServerList_t3;
    public static List<Connection> globalServerList_t4;
    public static List<Connection> coordinationServerList;
    public static List<Connection> proxyServerList;

    // host details
    protected static Integer hostPort;
    protected static Long hostID;
    protected static String hostIP;

    protected Long nextMessageID;

    public EdgeServer(String serverConfigStr){
        EdgeServer.serverConfigString = serverConfigStr;
        conf = ConfigFactory.parseResources("application.conf");
        init();
    }

    public static EdgeServer getInstance(String serverConfigStr){
        instance.compareAndSet(null, new EdgeServer(serverConfigStr));
        return instance.get();
    }

    private List<Connection> initLocalServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                            conf.getString(this.serverConfigString+".localServerList.svrIP_1"),
                            conf.getInt(this.serverConfigString+".localServerList.svrPort_1"));

        local_svr_list.add(svr_1);
        return local_svr_list;
    }

    private List<Connection> initProxyServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                conf.getString(this.serverConfigString+".proxyServerList.svrIP_1"),
                conf.getInt(this.serverConfigString+".proxyServerList.svrPort_1"));

        local_svr_list.add(svr_1);
        return local_svr_list;
    }

    private List<Connection> initGlobalServerList(Config conf){
        List<Connection> local_svr_list = new ArrayList<Connection>();

        Connection svr_1 = new Connection(
                            conf.getString(this.serverConfigString+".globalServerList.svrIP_1"),
                            conf.getInt(this.serverConfigString+".globalServerList.svrPort_1"));

        local_svr_list.add(svr_1);
        return local_svr_list;
    }

    private List<Connection> initCoordiantionServerList(Config conf){
        List<Connection> coordinationServerList = new ArrayList<Connection>();
        Connection svr_1 = new Connection(
                conf.getString(this.serverConfigString+".coordinationServerList.svrIP_1"),
                conf.getInt(this.serverConfigString+".coordinationServerList.svrPort_1"));

        coordinationServerList.add(svr_1);
        return coordinationServerList;

    }

    private List<Connection> initGlobalServerList_t1(Config conf){
        List<Connection> t1 = new ArrayList<Connection>();
        Connection svr_1 = new Connection(
                conf.getString("externalServerList.team1.svrIP_1"),
                conf.getInt("externalServerList.team1.svrPort_1"));
        Connection svr_2 = new Connection(
                conf.getString("externalServerList.team1.svrIP_2"),
                conf.getInt("externalServerList.team1.svrPort_2"));
        Connection svr_3 = new Connection(
                conf.getString("externalServerList.team1.svrIP_3"),
                conf.getInt("externalServerList.team1.svrPort_3"));
        Connection svr_4 = new Connection(
                conf.getString("externalServerList.team1.svrIP_4"),
                conf.getInt("externalServerList.team1.svrPort_4"));
        Connection svr_5 = new Connection(
                conf.getString("externalServerList.team1.svrIP_5"),
                conf.getInt("externalServerList.team1.svrPort_5"));

        t1.add(svr_1);
        t1.add(svr_2);
        t1.add(svr_3);
        t1.add(svr_4);
        t1.add(svr_5);
        return t1;
    }

    private List<Connection> initGlobalServerList_t3(Config conf){
        List<Connection> t3 = new ArrayList<Connection>();
        Connection svr_1 = new Connection(
                conf.getString("externalServerList.team3.svrIP_1"),
                conf.getInt("externalServerList.team3.svrPort_1"));
        Connection svr_2 = new Connection(
                conf.getString("externalServerList.team3.svrIP_2"),
                conf.getInt("externalServerList.team3.svrPort_2"));
        Connection svr_3 = new Connection(
                conf.getString("externalServerList.team3.svrIP_3"),
                conf.getInt("externalServerList.team3.svrPort_3"));
        Connection svr_4 = new Connection(
                conf.getString("externalServerList.team3.svrIP_4"),
                conf.getInt("externalServerList.team3.svrPort_4"));
        Connection svr_5 = new Connection(
                conf.getString("externalServerList.team3.svrIP_5"),
                conf.getInt("externalServerList.team3.svrPort_5"));

        t3.add(svr_1);
        t3.add(svr_2);
        t3.add(svr_3);
        t3.add(svr_4);
        t3.add(svr_5);
        return t3;
    }

    private List<Connection> initGlobalServerList_t4(Config conf){
        List<Connection> t4 = new ArrayList<Connection>();
        Connection svr_1 = new Connection(
                conf.getString("externalServerList.team4.svrIP_1"),
                conf.getInt("externalServerList.team4.svrPort_1"));
        Connection svr_2 = new Connection(
                conf.getString("externalServerList.team4.svrIP_2"),
                conf.getInt("externalServerList.team4.svrPort_2"));
        Connection svr_3 = new Connection(
                conf.getString("externalServerList.team4.svrIP_3"),
                conf.getInt("externalServerList.team4.svrPort_3"));
        Connection svr_4 = new Connection(
                conf.getString("externalServerList.team4.svrIP_4"),
                conf.getInt("externalServerList.team4.svrPort_4"));
        Connection svr_5 = new Connection(
                conf.getString("externalServerList.team4.svrIP_5"),
                conf.getInt("externalServerList.team4.svrPort_5"));

        t4.add(svr_1);
        t4.add(svr_2);
        t4.add(svr_3);
        t4.add(svr_4);
        t4.add(svr_5);
        return t4;
    }

    private void init() {
        if (conf == null) {
            throw new RuntimeException("server not configured!");
        }
        String tmp_ip = conf.getString(this.serverConfigString+".serverConfig.hostIP");
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
        globalServerList_t1 = initGlobalServerList_t1(conf);
        globalServerList_t3 = initGlobalServerList_t3(conf);
        globalServerList_t4 = initGlobalServerList_t4(conf);
        coordinationServerList = initCoordiantionServerList(conf);
        proxyServerList = initProxyServerList(conf);

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
