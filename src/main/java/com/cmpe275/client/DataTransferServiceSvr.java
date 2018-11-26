package com.cmpe275.client;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class DataTransferServiceTest extends DataTransferServiceGrpc.DataTransferServiceImplBase {
    private Server svr;

    public void listFiles(FileTransfer.RequestFileList req, StreamObserver<FileTransfer.FileList> responseObserver){
        responseObserver.onNext(getFileList());
        responseObserver.onCompleted();
    }

    public void getFileLocation(FileTransfer.FileInfo req, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
        responseObserver.onNext(getFileLocationdata());
        responseObserver.onCompleted();
    }

    public void downloadChunk(FileTransfer.ChunkInfo req, StreamObserver<FileTransfer.FileMetaData> responseObserver){
        for(int i=0; i<5; i++){
            responseObserver.onNext(getChunk(req,i));
        }
        responseObserver.onCompleted();
    }

    private FileTransfer.FileMetaData getChunk(FileTransfer.ChunkInfo req, int i) {
        System.out.println("downloadChunk works");
        FileTransfer.FileMetaData ret = FileTransfer.FileMetaData.newBuilder().setChunkId(i).setFileName("test0").build();
        return  ret;
    }

    private FileTransfer.FileLocationInfo getFileLocationdata() {
        System.out.println("getFileLocation works");
        FileTransfer.FileLocationInfo ret = FileTransfer.FileLocationInfo.newBuilder().setIsFileFound(true).setFileName("test0").build();
        return ret;
    }

    private FileTransfer.FileList getFileList() {
        System.out.println("list files works");
        FileTransfer.FileList ret = FileTransfer.FileList.newBuilder().addLstFileNames("Test0").build();
        return ret;
    }

    private void start() throws Exception {
        svr = ServerBuilder.forPort(10000).addService(new DataTransferServiceTest())
                .build();

        System.out.println("-- starting server");
        svr.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                DataTransferServiceTest.this.stop();
            }
        });
    }

    private void stop() {
        svr.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        svr.awaitTermination();
    }
    public static void main(){
        DataTransferServiceTest testSvr = new DataTransferServiceTest();
        try {
            testSvr.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            testSvr.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
