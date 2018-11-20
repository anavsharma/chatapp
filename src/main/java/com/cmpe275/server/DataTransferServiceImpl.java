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
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {
    private static Logger LOG = LoggerFactory.getLogger(DataTransferServiceImpl.class.getName());
    private EdgeServer edgeServer;
    private static List<ManagedChannel> localChannels = new ArrayList<ManagedChannel>();
    private static List<ManagedChannel> globalChannels = new ArrayList<ManagedChannel>();
    private static List<ManagedChannel> coordinationChannels = new ArrayList<ManagedChannel>();

    DataTransferServiceImpl(EdgeServer edgeServer){
        super();
        this.edgeServer = edgeServer;
        initConnections();
    }

    public static void initConnections(){
        for(Connection c : EdgeServer.localServerList){
            ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext().build();
            localChannels.add(ch);
        }

        for(Connection c: EdgeServer.globalServerList){
            ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext().build();
            globalChannels.add(ch);
        }

        for(Connection c: EdgeServer.coordinationServerList){
            ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext().build();
            coordinationChannels.add(ch);
        }
    }

    public void requestFileInfo(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
        responseObserver.onNext(getAllClusterInfo(request));
    }

    private FileTransfer.FileLocationInfo getAllClusterInfo(FileTransfer.FileInfo request) {
        int i = 0;
        for(i = 0; i< globalChannels.size(); i++){
            FileTransfer.FileLocationInfo locationInfo = getGlobalClusterInfo(request, i);
            if(locationInfo.getIsFileFound()){
                return locationInfo;
            }
        }
        return FileTransfer.FileLocationInfo.newBuilder().setIsFileFound(false).build();
    }

    public FileTransfer.FileLocationInfo getGlobalClusterInfo(FileTransfer.FileInfo req, int index){
        ManagedChannel ch = coordinationChannels.get(index);
        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch);
        ListenableFuture<FileTransfer.FileLocationInfo> res = stub.getFileLocation(req);
        Futures.addCallback(res, new FutureCallback<FileTransfer.FileLocationInfo>() {
            public void onSuccess(@Nullable FileTransfer.FileLocationInfo fileLocationInfo) {
                LOG.debug("GetLocalClusterInfo: Got a response from local.");
            }

            public void onFailure(Throwable throwable) {
                LOG.error("Could not get file details.");
            }
        });
        FileTransfer.FileLocationInfo locationInfo = null;
        try{
            locationInfo = res.get();
        } catch (Exception e){
            e.printStackTrace();
            LOG.error("Error:");
        }
        return locationInfo;
    }

    public FileTransfer.FileLocationInfo getLocalClusterInfo(FileTransfer.FileInfo req){
        ManagedChannel ch = coordinationChannels.get(0);
        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch);
        ListenableFuture<FileTransfer.FileLocationInfo> res = stub.getFileLocation(req);
        Futures.addCallback(res, new FutureCallback<FileTransfer.FileLocationInfo>() {
            public void onSuccess(@Nullable FileTransfer.FileLocationInfo fileLocationInfo) {
                LOG.debug("GetLocalClusterInfo: Got a response from local.");
            }

            public void onFailure(Throwable throwable) {
                LOG.error("Could not get file details.");
            }
        });
        FileTransfer.FileLocationInfo locationInfo = null;
        try{
            locationInfo = res.get();
        } catch (Exception e){
            e.printStackTrace();
            LOG.error("Error:");
        }

        return locationInfo;

    }

    public void getFileLocation(FileTransfer.FileInfo request, StreamObserver<FileTransfer.FileLocationInfo> responseObserver){
       responseObserver.onNext(getLocalClusterInfo(request));
       responseObserver.onCompleted();
    }

    public void listFiles(FileTransfer.RequestFileList request, StreamObserver<FileTransfer.FileList> responseObserver){
        FileTransfer.FileListOrBuilder files = FileTransfer.FileList.newBuilder();
        if(request.getIsClient()){
            //forward request to coordination server
            responseObserver.onNext(getFileLists(request));
            responseObserver.onCompleted();
            //forward request to other clusters
        } else {
            //forward request to coordination server
            responseObserver.onNext(getFileListLocal(request));
            responseObserver.onCompleted();
        }
    }

    private FileTransfer.FileList getFileLists(FileTransfer.RequestFileList request){
        FileTransfer.RequestFileList globalRequest = FileTransfer.RequestFileList.newBuilder().setIsClient(false).build();
        FileTransfer.FileList localList = getFileListLocal(request);
        FileTransfer.FileList globalList0 = getFileListGlobal(globalRequest, 0);
        FileTransfer.FileList globalList1 = getFileListGlobal(globalRequest, 1);
        FileTransfer.FileList globalList2 = getFileListGlobal(globalRequest, 2);

        FileTransfer.FileList allFiles = FileTransfer.FileList.newBuilder()
                                            .mergeFrom(localList)
                                            .mergeFrom(globalList0)
                                            .mergeFrom(globalList1)
                                            .mergeFrom(globalList2)
                                            .build();
        return allFiles;
    }

    private FileTransfer.FileList getFileListLocal(FileTransfer.RequestFileList request) {
        ManagedChannel ch = coordinationChannels.get(0);
        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch);
        ListenableFuture<FileTransfer.FileList>  res = stub.listFiles(request);
        Futures.addCallback(res, new FutureCallback<FileTransfer.FileList>() {
            public void onSuccess(@Nullable FileTransfer.FileList fileList) {
                LOG.debug("File list received from coordination server.");
            }

            public void onFailure(Throwable throwable) {
                LOG.error("File list not received from coordination server.");
            }
        });
        FileTransfer.FileList files = null;
        try{
            files = res.get();
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("Error:");
        }
        return files;
    }

    private FileTransfer.FileList getFileListGlobal(FileTransfer.RequestFileList request, int i){
        ManagedChannel ch0 = globalChannels.get(i);
        DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(ch0);
        ListenableFuture<FileTransfer.FileList> res = stub.listFiles(request);
        Futures.addCallback(res, new FutureCallback<FileTransfer.FileList>() {
            public void onSuccess(@Nullable FileTransfer.FileList fileList) {
                LOG.debug("File list received from coordination server.");
            }

            public void onFailure(Throwable throwable) {
                LOG.error("File list not received from coordination server.");
            }
        });
        FileTransfer.FileList files = null;
        try{
            files = res.get();
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("Error:");
        }
        return files;
    }

}
