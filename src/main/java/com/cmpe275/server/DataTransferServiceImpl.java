package com.cmpe275.server;

/**
 * copyright 2018, gash
 *
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import com.cmpe275.generated.FileResponse;
import com.cmpe275.util.Connection;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {
    private static Logger LOG = LoggerFactory.getLogger(DataTransferServiceImpl.class.getName());
    private EdgeServer edgeServer;
    private static List<ManagedChannel> localChannels = new ArrayList<ManagedChannel>();
    private static List<ManagedChannel> coordinationChannels = new ArrayList<ManagedChannel>();

    DataTransferServiceImpl(EdgeServer edgeServer){
        super();
        this.edgeServer = edgeServer;
        initConnections();
    }

    public static void initConnections(){
        for(Connection c : EdgeServer.localServerList){
            ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext(true).build();
            localChannels.add(ch);
        }

        for(Connection c: EdgeServer.coordinationServerList){
            ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext(true).build();
            coordinationChannels.add(ch);
        }
    }

    public void requestFileInfo(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
        responseObserver.onNext(getAllClusterInfo(request));
    }

    private FileTransfer.FileLocationInfo getAllClusterInfo(FileTransfer.FileInfo request) {
        int i = 0;
        Connection t1 = edgeServer.globalServerList_t1.get(getIndex(5));
        Connection t3 = edgeServer.globalServerList_t3.get(getIndex(5));
        Connection t4 = edgeServer.globalServerList_t4.get(getIndex(5));
        FileTransfer.FileLocationInfo locationInfo_t1 = getGlobalClusterInfo(request, t1);
        FileTransfer.FileLocationInfo locationInfo_t3 = getGlobalClusterInfo(request, t3);
        FileTransfer.FileLocationInfo locationInfo_t4 = getGlobalClusterInfo(request, t4);
        FileTransfer.FileLocationInfo allFileLocations = FileTransfer.FileLocationInfo.newBuilder()
                            .setFileName(locationInfo_t1.getFileName())
                            .mergeFrom(locationInfo_t1)
                            .mergeFrom(locationInfo_t3)
                            .mergeFrom(locationInfo_t4)
                            .setIsFileFound(locationInfo_t1.getIsFileFound()||locationInfo_t3.getIsFileFound()||locationInfo_t4.getIsFileFound())
                            .build();
        return FileTransfer.FileLocationInfo.newBuilder().setIsFileFound(false).build();
    }

    public FileTransfer.FileLocationInfo getGlobalClusterInfo(FileTransfer.FileInfo req, Connection c){
        ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext(true).build();
        DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(ch);
        FileTransfer.FileLocationInfoOrBuilder retVal = FileTransfer.FileLocationInfo.newBuilder();
        try{
            FileTransfer.FileLocationInfo fileLocationInfo = stub.requestFileInfo(req);
            ((FileTransfer.FileLocationInfo.Builder) retVal).mergeFrom(fileLocationInfo);
        } catch  (StatusRuntimeException e){
            LOG.error("Runtime Exception: "+e.getMessage());
        }
//        ListenableFuture<FileTransfer.FileLocationInfo> res = stub.getFileLocation(req);
//        Futures.addCallback(res, new FutureCallback<FileTransfer.FileLocationInfo>() {
//            public void onSuccess(@Nullable FileTransfer.FileLocationInfo fileLocationInfo) {
//                LOG.debug("GetLocalClusterInfo: Got a response from local.");
//            }
//
//            public void onFailure(Throwable throwable) {
//                LOG.error("Could not get file details.");
//            }
//        });
//        FileTransfer.FileLocationInfo locationInfo = null;
//        try{
//            locationInfo = res.get();
//        } catch (Exception e){
//            e.printStackTrace();
//            LOG.error("Error:");
//        }
//        return locationInfo;
        return ((FileTransfer.FileLocationInfo.Builder) retVal).build();
    }

    public FileTransfer.FileLocationInfo getLocalClusterInfo(FileTransfer.FileInfo req){
        ManagedChannel ch = coordinationChannels.get(0);
        DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(ch);
//        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch);
//        ListenableFuture<FileTransfer.FileLocationInfo> res = stub.getFileLocation(req);
//        Futures.addCallback(res, new FutureCallback<FileTransfer.FileLocationInfo>() {
//            public void onSuccess(@Nullable FileTransfer.FileLocationInfo fileLocationInfo) {
//                LOG.debug("GetLocalClusterInfo: Got a response from local.");
//            }
//
//            public void onFailure(Throwable throwable) {
//                LOG.error("Could not get file details.");
//            }
//        });
//        FileTransfer.FileLocationInfo locationInfo = null;
//        try{
//            locationInfo = res.get();
//        } catch (Exception e){
//            e.printStackTrace();
//            LOG.error("Error:");
//        }
//
//        return locationInfo;
        FileTransfer.FileLocationInfoOrBuilder retVal = FileTransfer.FileLocationInfo.newBuilder();
        try{
            FileTransfer.FileLocationInfo fileLocationInfo = stub.requestFileInfo(req);
            ((FileTransfer.FileLocationInfo.Builder) retVal).mergeFrom(fileLocationInfo);
        } catch  (StatusRuntimeException e){
            LOG.error("Runtime Exception: "+e.getMessage());
        }
        return ((FileTransfer.FileLocationInfo.Builder) retVal).build();
    }

    public void getFileLocation(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
       responseObserver.onNext(getLocalClusterInfo(request));
       responseObserver.onCompleted();
    }
    @Override
    public void listFiles(FileTransfer.RequestFileList request, StreamObserver<FileTransfer.FileList> responseObserver){
        FileTransfer.FileListOrBuilder files = FileTransfer.FileList.newBuilder();
        if(request.getIsClient()){
            //forward request to coordination server
            responseObserver.onNext(getFileLists(request));
            responseObserver.onCompleted();
            //forward request to other clusters
        }
//        else {
//            //forward request to coordination server
//            responseObserver.onNext(getFileListLocal(request));
//            responseObserver.onCompleted();
//        }
    }

    private FileTransfer.FileList getFileLists(FileTransfer.RequestFileList request){
        FileTransfer.RequestFileList globalRequest = FileTransfer.RequestFileList.newBuilder().setIsClient(false).build();
        FileTransfer.FileList localList = getFileListLocal(request);
        Set<FileTransfer.FileList> consolidatedList = new HashSet<FileTransfer.FileList>();
        for(int i=0; i<5; i++){
            FileTransfer.FileList globalList1 = getFileListGlobal(globalRequest, edgeServer.globalServerList_t1.get(i));
            if (globalList1.getLstFileNamesCount() > 0) {
                consolidatedList.add(globalList1);
            }
        }
        for(int i=0; i<5; i++){
            FileTransfer.FileList globalList3 = getFileListGlobal(globalRequest, edgeServer.globalServerList_t3.get(i));
            if (globalList3.getLstFileNamesCount() > 0) {
                consolidatedList.add(globalList3);
            }
        }
        for(int i=0; i<5; i++){
            FileTransfer.FileList globalList2 = getFileListGlobal(globalRequest, edgeServer.globalServerList_t4.get(i));
            if (globalList2.getLstFileNamesCount() > 0) {
                consolidatedList.add(globalList2);
            }
        }
        List<String> files = new ArrayList<String>();
        FileTransfer.FileListOrBuilder retList = FileTransfer.FileList.newBuilder();
        for(FileTransfer.FileList fileList: consolidatedList){
            ((FileTransfer.FileList.Builder) retList).mergeFrom(fileList);
        }
        ((FileTransfer.FileList.Builder) retList).mergeFrom(localList);

        return ((FileTransfer.FileList.Builder) retList).build();
    }

    private FileTransfer.FileList getFileListLocal(FileTransfer.RequestFileList request) {
        ManagedChannel ch = coordinationChannels.get(0);
        DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(ch);
//        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch);
//        ListenableFuture<FileTransfer.FileList>  res = stub.listFiles(request);
//        Futures.addCallback(res, new FutureCallback<FileTransfer.FileList>() {
//            public void onSuccess(@Nullable FileTransfer.FileList fileList) {
//                LOG.debug("File list received from coordination server.");
//            }
//
//            public void onFailure(Throwable throwable) {
//                LOG.error("File list not received from coordination server.");
//            }
//        });
//
//        FileTransfer.FileList files = null;
//        try{
//            files = res.get();
//        }catch (Exception e){
//            e.printStackTrace();
//            LOG.error("Error:");
//        }
//        ch.shutdown();
//        return files;
        FileTransfer.FileListOrBuilder retVal = FileTransfer.FileList.newBuilder();
        try{
            FileTransfer.FileList res = stub.listFiles(request);
            ((FileTransfer.FileList.Builder) retVal).mergeFrom(res);
        }catch(StatusRuntimeException e){
            LOG.error("Runtime Exception: "+e.getMessage());
        }
        return ((FileTransfer.FileList.Builder) retVal).build();
    }

    private FileTransfer.FileList getFileListGlobal(FileTransfer.RequestFileList request, Connection c){
        ManagedChannel ch0 = ManagedChannelBuilder.forAddress(c.ipAddress,c.port).usePlaintext(true).build();
        DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(ch0);
        FileTransfer.FileListOrBuilder retVal = FileTransfer.FileList.newBuilder();
        try{
            FileTransfer.FileList res = stub.listFiles(request);
            ((FileTransfer.FileList.Builder) retVal).mergeFrom(res);
        }catch(StatusRuntimeException e){
            LOG.error("Runtime Exception: "+e.getMessage());
        }
        return ((FileTransfer.FileList.Builder) retVal).build();
//        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch0);
//        ListenableFuture<FileTransfer.FileList> res = stub.listFiles(request);
//        Futures.addCallback(res, new FutureCallback<FileTransfer.FileList>() {
//            public void onSuccess(FileTransfer.FileList fileList) {
//                LOG.debug("File list received from coordination server.");
//            }
//
//            public void onFailure(Throwable throwable) {
//                LOG.error("File list not received from coordination server.");
//            }
//        });
//        FileTransfer.FileList files = null;
//        try{
//            files = res.get();
//        }catch (Exception e){
//            e.printStackTrace();
//            LOG.error("Error:");
//        }
//        return files;
    }

    //TODO Request File Upload needs to be implemented

    public static int getIndex(int i){
        Random rand = new Random();
        return rand.nextInt(i);
    }

}
