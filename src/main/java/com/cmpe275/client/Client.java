package com.cmpe275.client;

import com.cmpe275.generated.*;
import com.cmpe275.util.Connection;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Client {
    private static Logger LOG = LoggerFactory.getLogger(Client.class.getName());
    private static Config conf;
    private static List<Connection> edgeServerList;
    private static List<ManagedChannel> chList;
    private static List<DataTransferServiceGrpc.DataTransferServiceFutureStub> stubs;
    private static Long rpcCount;
    private static Semaphore limiter;

    private static long clientID;
    private static int clientPort;

    private static void init(){
        Client.conf = ConfigFactory.parseResources("application.conf");
        LOG.debug("Loaded config file.");
        Client.edgeServerList = initEdgeServerList(conf);
        Client.chList = initChList();
        Client.stubs = initStubs();
        Client.rpcCount = 0l;
        limiter = new Semaphore(100);
        LOG.debug("Initialized client data.");
    }

    private static List<Connection> initEdgeServerList(Config conf){
        List<Connection> serverList = new ArrayList<Connection>();
        Connection svr1 = new Connection(
                conf.getString("client1.localServerList.svrIP_1"),
                conf.getInt("client1.localServerList.svrPort_1"));
        Connection svr2 = new Connection(
                conf.getString("client1.localServerList.svrIP_2"),
                conf.getInt("client1.localServerList.svrPort_2"));
        Connection svr3 = new Connection(
                conf.getString("client1.localServerList.svrIP_3"),
                conf.getInt("client1.localServerList.svrPort_3"));
        serverList.add(svr1);
        serverList.add(svr2);
        serverList.add(svr3);

        return serverList;
    }

    private static List<ManagedChannel>initChList(){
        List<ManagedChannel> chList = new ArrayList<ManagedChannel>();
        ManagedChannel ch0 = ManagedChannelBuilder.forAddress(
                Client.edgeServerList.get(0).ipAddress,
                Client.edgeServerList.get(0).port
        ).usePlaintext(true).build();
        ManagedChannel ch1 = ManagedChannelBuilder.forAddress(
                Client.edgeServerList.get(1).ipAddress,
                Client.edgeServerList.get(1).port
        ).usePlaintext(true).build();
        ManagedChannel ch2 = ManagedChannelBuilder.forAddress(
                Client.edgeServerList.get(2).ipAddress,
                Client.edgeServerList.get(2).port
        ).usePlaintext(true).build();
        chList.add(ch0);
        chList.add(ch1);
        chList.add(ch2);
        return chList;
    }

    private static List<DataTransferServiceGrpc.DataTransferServiceFutureStub> initStubs(){
        List<DataTransferServiceGrpc.DataTransferServiceFutureStub> stubs = new ArrayList<DataTransferServiceGrpc.DataTransferServiceFutureStub>();
        for(ManagedChannel ch: Client.chList){
            stubs.add(DataTransferServiceGrpc.newFutureStub(ch));
        }
        return stubs;
    }

    //non blocking stub
    private static FileTransfer.FileLocationInfo doRequestFileInfo(String filename) {
        try{
            limiter.acquire();
        } catch (InterruptedException e){
            e.printStackTrace();
            LOG.error("Could not initiate request due to too many requests.");
        }
        FileTransfer.FileInfo req = FileTransfer.FileInfo.newBuilder().setFileName(filename).build();
        LOG.debug("File Info requested.");
        ListenableFuture<FileTransfer.FileLocationInfo> res = stubs.get(getIndex()).getFileLocation(req);
        Futures.addCallback(res, new FutureCallback<FileTransfer.FileLocationInfo>() {
            public void onSuccess(@Nullable FileTransfer.FileLocationInfo fileLocationInfo) {
                rpcCount++;
                limiter.release();
                LOG.debug("File Info request succeeded.");
            }

            public void onFailure(Throwable throwable) {
                rpcCount++;
                limiter.release();
                LOG.error("File list request failed.");
            }
        });
        FileTransfer.FileLocationInfo fileLocationInfo = null;
        try {
            fileLocationInfo = res.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return fileLocationInfo;
    }

    //non blocking stub
    private static void doListFiles() {
        System.out.println("Reached Here..");
        FileTransfer.RequestFileList req = FileTransfer.RequestFileList.newBuilder().setIsClient(true).build();
        LOG.debug("File list requested.");
        ListenableFuture<FileTransfer.FileList> res = stubs.get(getIndex()).withDeadlineAfter(25000, TimeUnit.MILLISECONDS).listFiles(req);
        Futures.addCallback(res, new FutureCallback<FileTransfer.FileList>() {
            public void onSuccess(FileTransfer.FileList resFileList) {
                System.out.println("Successful.");
                for(int i = 0; i < resFileList.getLstFileNamesCount(); ++i){
                    rpcCount++;
                    limiter.release();
                    LOG.debug("File list request succeeded.");
                    System.out.println("GOT RESULT");
                    System.out.println(resFileList.getLstFileNames(i));
                    chList.get(getIndex()).shutdown();
                }
            }
            public void onFailure(Throwable throwable) {
                limiter.release();
                LOG.error("File list request failed.");
                chList.get(getIndex()).shutdown();
            }
        });
    }

    //this uses a blocking stub
    private static FileTransfer.FileMetaData doDownloadChunk(FileTransfer.ChunkInfo chunkInfo, String proxyAddr, int port){
        try {
            limiter.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error("Could not initiate request due to too many requests.");
        }
        LOG.debug("Download Chunk started.");
        ManagedChannel ch = ManagedChannelBuilder.forAddress(proxyAddr, port).build();
        DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(ch);
        Iterator<FileTransfer.FileMetaData> fileMetaDataIterator;
        FileTransfer.FileMetaData fileMetaData= FileTransfer.FileMetaData.newBuilder().setFileName(chunkInfo.getFileName()).setChunkId(chunkInfo.getChunkId()).buildPartial();
        ByteString accumulator = null;
        try{
            fileMetaDataIterator = stub.downloadChunk(chunkInfo);
            rpcCount++;
            while(fileMetaDataIterator.hasNext()){
                FileTransfer.FileMetaData fmd = fileMetaDataIterator.next();
                accumulator.concat(fmd.getData());
            }
        } catch (Exception e){
            LOG.error("Download Chunk Failed.");
        }
        limiter.release();
        return FileTransfer.FileMetaData.newBuilder().setFileName(chunkInfo.getFileName()).setChunkId(chunkInfo.getChunkId()).setData(accumulator).build();
    }

    public static void main(String[] args){
        Client.init();
        System.out.println("Started the client");
        if(args.length != 2){
            if (args.length == 1) {
                    System.out.println("Listing files available...");
                    doListFiles();
                } else {
                    System.out.println("Needs 2 parameters: 1. Catalog, 2. Download File 3.Upload File");
                }
            } else{
                if(args.length ==2) switch (Integer.parseInt(args[0])) {
                    case 2:
                        doDownload(args[1]);
                        break;
                    case 3:
                        doUploadFile(args[1], "./data/");
                        break;
                }
            }
        Scanner s = new Scanner(System.in);
        s.nextLine();
        System.out.println(s);
    }

    public static void doUploadFile(String filename, String filepath){
        //TODO Add a config variable for chunk size,
        byte[] data = {};
        try{
            File file = new File(filepath+"/"+filename);
            int size = (int) file.length();
            data = new byte[size];
            InputStream is = new FileInputStream(file);
            is.read(data);
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileUploadRequest req = FileUploadRequest.newBuilder().setFileName(filename).setMaxChunks(4).setRequestId(new Random().nextInt()).build();
        FileResponse res = doInitiateFileUpload(req);
        if(res.getIsFound()){
            List<ChunkData> chunkDataList = res.getChunksList();
            for(ChunkData cd: chunkDataList){
                doUploadFileChunk(cd);
            }
        }
    }

    public static void doDownload(String filename){
        ByteString fileData = ByteString.EMPTY;
        FileTransfer.FileLocationInfo res = doRequestFileInfo(filename);
        if(res.getIsFileFound()){
            List<FileTransfer.ProxyInfo> proxys = res.getLstProxyList();
            long maxChunks = res.getMaxChunks();
            int currentChunkCount = 0;
            int [] chunkFound = new int[(int)maxChunks];
            for(int i : chunkFound){
                chunkFound[i] = 0;
            }
            //try getting the chunks
            for(int i = 0; i<= maxChunks; i++){
                FileTransfer.ChunkInfo chunkInfo = FileTransfer.ChunkInfo.newBuilder()
                                                    .setFileName(filename)
                                                    .setChunkId(i)
                                                    .setStartSeqNum(0l)
                                                    .build();
                FileTransfer.FileMetaData fileMetaData = doDownloadChunk(chunkInfo, proxys.get(0).getIp(), Integer.parseInt(proxys.get(0).getPort()));
                if(!fileMetaData.isInitialized()){
                    for(FileTransfer.ProxyInfo proxy: proxys){
                        fileMetaData = doDownloadChunk(chunkInfo, proxy.getIp(), Integer.parseInt(proxy.getPort()));
                        if(fileMetaData.isInitialized())
                            break;
                    }
                }
                fileData.concat(fileMetaData.getData());
            }
        }
        try{
            File downloadedFile = new File("./receivedData/"+filename);
            if(downloadedFile.createNewFile()){
                FileOutputStream out = new FileOutputStream("./"+filename);
                out.write(fileData.toByteArray());
                out.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }


    }

    public static FileResponse doInitiateFileUpload(FileUploadRequest req){
        try {
            limiter.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error("Could not initiate request due to too many requests.");
        }
        LOG.debug("Initiate File Upload started.");
        ManagedChannel ch = chList.get(getIndex());
        clusterServiceGrpc.clusterServiceFutureStub stub = clusterServiceGrpc.newFutureStub(ch);
        ListenableFuture<FileResponse> res = stub.initiateFileUpload(req);
        Futures.addCallback(res, new FutureCallback<FileResponse>(){
            public void onSuccess(@Nullable FileResponse fileResponse){
                rpcCount++;
                limiter.release();
                LOG.debug("Initiate file upload was successful.");
            }
            public void onFailure(Throwable throwable){
                rpcCount++;
                limiter.release();
                LOG.error("Initiate file upload failed.");
            }
        });
        FileResponse fileRes = null;
        try{
            fileRes = res.get();
        } catch (Exception e){
            e.printStackTrace();
            LOG.error("Error:");
        }
        return fileRes;

    }

    public static void doUploadFileChunk(ChunkData chunkData){
        try {
            limiter.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error("Could not initiate request due to too many requests.");
        }
        LOG.debug("Initiate File Upload started.");
        int index = getIndex();
        ManagedChannel ch = chList.get(index);
        clusterServiceGrpc.clusterServiceStub asyncStub = clusterServiceGrpc.newStub(ch);
        Chunk chunk = Chunk.newBuilder()
                        .setFileName(chunkData.getFileName())
                        .setChunkId(chunkData.getChunkId())
                        .setMaxChunks(chunkData.getMaxChunks())
                        .setSeqNum(0)
                        .setSeqMax(0)
                        .build();
        StreamObserver<ChunkAck> responseObserver = new StreamObserver<ChunkAck>() {
            public void onNext(ChunkAck chunkAck) {
                LOG.debug("Successfully written chunk: "+chunkAck.getFileName()+chunkAck.getChunkId());
            }

            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                LOG.error("Upload File Chunk failed:", status);
            }

            public void onCompleted() {
                LOG.debug("Upload File Chunk completed.");
            }
        };

        StreamObserver<Chunk> requestObserver = asyncStub.uploadFileChunk(responseObserver);
        try{
            requestObserver.onNext(chunk);
            limiter.release();
            rpcCount++;

        } catch(RuntimeException e){
            requestObserver.onError(e);
        }

        requestObserver.onCompleted();
    }

    public static FileResponse doIsFilePresent(FileQuery req){
        try {
            limiter.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error("Could not initiate request due to too many requests.");
        }
        LOG.debug("Is File Present started.");
        ManagedChannel ch = chList.get(getIndex());
        clusterServiceGrpc.clusterServiceFutureStub stub = clusterServiceGrpc.newFutureStub(ch);
        ListenableFuture<FileResponse> res = stub.withDeadlineAfter(5000, TimeUnit.MILLISECONDS).isFilePresent(req);
        Futures.addCallback(res, new FutureCallback<FileResponse>() {
            public void onSuccess(@Nullable FileResponse fileResponse) {
                rpcCount++;
                limiter.release();
                LOG.debug("successfully completed request.");
            }

            public void onFailure(Throwable throwable) {
                LOG.error("IsFilePresent failed. "+Status.fromThrowable(throwable));
            }
        });
        FileResponse fileRes = null;
        try{
            fileRes = res.get();
        } catch (Exception e){
            e.printStackTrace();
            LOG.error("Error:");
        }
        return fileRes;
    }

    public static int getIndex(){
        Random rand = new Random();
        return rand.nextInt(1);
    }

}
