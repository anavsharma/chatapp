package com.cmpe275.server;

import com.cmpe275.generated.*;
import com.cmpe275.proxy.ProxyServer;
import com.cmpe275.util.Connection;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ClusterServiceImpl extends clusterServiceGrpc.clusterServiceImplBase {
    private static Logger LOG = LoggerFactory.getLogger(ClusterServiceImpl.class.getName());
    private EdgeServer edgeServer;
    private static List<ManagedChannel> localChannels = new ArrayList<ManagedChannel>();
    private static List<ManagedChannel> globalChannels = new ArrayList<ManagedChannel>();
    private static List<ManagedChannel> coordinationChannels = new ArrayList<ManagedChannel>();
    private static List<ManagedChannel> proxyChannels = new ArrayList<ManagedChannel>();

    ClusterServiceImpl(EdgeServer edgeServer){
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

        for(Connection c: EdgeServer.proxyServerList){
            ManagedChannel ch = ManagedChannelBuilder.forAddress(c.ipAddress, c.port).usePlaintext().build();
            proxyChannels.add(ch);
        }
    }

    public void Liveliness(){

    }

    public void updateChunkData(){

    }

    public void initiateFileUpload(FileUploadRequest request, StreamObserver<FileResponse> responseObserver){
        responseObserver.onNext(initUpload(request));
        responseObserver.onCompleted();
    }

    private FileResponse initUpload(FileUploadRequest request) {
        String filename = request.getFileName();
        long maxChunks = request.getMaxChunks();
        LOG.debug("Processing init file upload for request: ", request.getRequestId());

        ManagedChannel ch = coordinationChannels.get(0);
        clusterServiceGrpc.clusterServiceFutureStub stub = clusterServiceGrpc.newFutureStub(ch);
        ListenableFuture<FileResponse> res = stub.initiateFileUpload(request);
        Futures.addCallback(res, new FutureCallback<FileResponse>() {
            public void onSuccess(@Nullable FileResponse fileResponse) {
                LOG.debug("Received response.");
            }

            public void onFailure(Throwable throwable) {
                LOG.error("Initiate file upload failed. ", throwable.getMessage());
            }
        });
        FileResponse fileRes = null;
        try {
            fileRes = res.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return fileRes;
    }

    public StreamObserver<Chunk> uploadFileChunk(final StreamObserver<ChunkAck> responseObserver){
        return new StreamObserver<Chunk>() {
            long lastChunkId = 0;
            String filename = "";
            ByteString chunkData = ByteString.EMPTY;
            long maxChunks = 100;
            long lastSeq = 0;
            long maxSeq = 100;

            public void onNext(Chunk chunk) {
                LOG.debug("Processing upload for chunk {1} of file {2}", chunk.getChunkId(), chunk.getFileName());
                chunkData.concat(chunk.getData());
                filename = chunk.getFileName();
                lastChunkId = chunk.getChunkId();
                maxChunks = chunk.getMaxChunks();
                lastSeq = chunk.getSeqNum();
                maxSeq = chunk.getSeqMax();
            }

            public void onError(Throwable throwable) {
                LOG.error("There was an error in uploadFileChunk : ", throwable.getMessage());
            }

            public void onCompleted() {
                responseObserver.onNext(forwardRequest());
            }

            public ChunkAck forwardRequest(){
                LOG.debug("Recieved chunk.");
                ChunkAck ack = ChunkAck.newBuilder().setChunkId(lastChunkId).setDone(true).setFileName(filename).build();
                return ack;
            }
        };
    }
}
