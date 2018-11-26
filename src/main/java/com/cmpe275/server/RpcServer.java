package com.cmpe275.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class RpcServer {
    private DataTransferServiceImpl dataTransferService;
    private ClusterServiceImpl clusterService;
    private EdgeServer edgeServer;
    private Server server;

    RpcServer(String svrConf){
        edgeServer = new EdgeServer(svrConf).getInstance(svrConf);
        dataTransferService = new DataTransferServiceImpl(this.edgeServer);
        clusterService = new ClusterServiceImpl(this.edgeServer);
        server = ServerBuilder.forPort(edgeServer.getServerPort())
                .addService(dataTransferService)
                .addService(clusterService)
                .build();
    }

    private void start(){
        try {
            server.start();
            System.out.println("grpc server started");
            server.awaitTermination();
        }catch (IOException e){
            System.err.println("IO excpetion with starting the grpc server!");
            e.printStackTrace();
            System.exit(1);
        }catch (InterruptedException e){
            System.err.println("Interrupt excpetion with awaiting termination of the grpc server!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected void stop() {
        server.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        server.awaitTermination();
    }

    public static void main(String[] args){
        if(args.length != 1){
            System.out.println("Enter one of svr1, svr2, svr3 or svr4 as the command line arg to start.");
        }
        RpcServer myServer= new RpcServer(args[0]);
        myServer.start();
        try {
            myServer.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}