package com.cmpe275.client;

import com.cmpe275.generated.*;
import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class Client {
    protected static Logger LOG = LoggerFactory.getLogger("server");
    protected static Config conf;
    private static long clientID = 12345;
    private static int port = 27009;
    private static String serverIP;
    private static Long serverPort;
    private long rpcCount = 10;
    private String filename = "testFile";

    private static ManagedChannel ch;
    private static DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub;

    public static void configure(){
        Client.conf = ConfigFactory.load();
        LOG.debug("Loaded config file.");
    }
    private static void init(){
        Client.clientID = conf.getLong("client.clientID");
        Client.port = conf.getInt("client.clientPort");
        Client.serverIP = conf.getString("client.serverIP");
        Client.serverPort = conf.getLong("client.serverPort");
        Client.ch = ManagedChannelBuilder.forAddress("localhost", Client.port).usePlaintext().build();
        Client.blockingStub = DataTransferServiceGrpc.newBlockingStub(ch);
        LOG.debug("Initialized client data.");
    }

    private static FileLocationInfo doRequestFileInfo(DataTransferServiceGrpc.DataTransferServiceBlockingStub stub, String filename){
        FileInfo req = FileInfo.newBuilder().setFileid(filename).build();
        FileLocationInfo res = stub.requestFileInfo(req);
        LOG.debug("Sent: "+req.toString());
        LOG.debug("Recieved: "+res.toString());
        return res;
    }

    private static FileLocationInfo doGetFileInfo(DataTransferServiceGrpc.DataTransferServiceBlockingStub stub){
        FileInfo req = FileInfo.newBuilder().setFileid("testFile").build();
        FileLocationInfo res = stub.getFileLocation(req);
        LOG.debug("Sent: "+req.toString());
        LOG.debug("Recieved: "+res.toString());
        return res;
    }

    private static void downloadFile(DataTransferServiceGrpc.DataTransferServiceBlockingStub stub, String filename){
        FileLocationInfo res = doRequestFileInfo(Client.blockingStub, filename);
        int i = 0;
        ByteString allFileParts = ByteString.EMPTY;
        for(ChunkLocationInfo chunkLocationInfo: res.getLstChunkLocationList()){
            ChunkInfo chunkInfo = ChunkInfo.newBuilder()
                                    .setFileName(filename)
                                    .setChunkId(chunkLocationInfo.getChunkId())
                                    .setStartSeqNum(i)
                                    .build();
            i += 1;
            ByteString filePart = doDownloadChunk(chunkInfo, chunkLocationInfo.getIp(), Integer.parseInt(chunkLocationInfo.getPort()));
            allFileParts.concat(filePart);
        }
        File myFile = new File("./"+res.getFileName());
        try {
            myFile.createNewFile();
            FileOutputStream opStream = new FileOutputStream(myFile);
            opStream.write(allFileParts.toByteArray());
            System.out.print("Got the file: "+ filename);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private static ByteString doDownloadChunk(ChunkInfo chunkInfo, String ip, Integer port){
        ManagedChannel new_ch = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
        DataTransferServiceGrpc.DataTransferServiceBlockingStub newStub = DataTransferServiceGrpc.newBlockingStub(new_ch);
        Iterator<FileMetaData> fIter = newStub.downloadChunk(chunkInfo);
        List<FileMetaData> fileMetaDataList = new ArrayList<FileMetaData>();
        ByteString data = ByteString.EMPTY;
        while (fIter.hasNext()){
            FileMetaData fileMetaData = fIter.next();
            fileMetaDataList.add(fileMetaData);
            data.concat(fileMetaData.getData());
        }
        return data;
    }

    public static void main(String[] args){
        Client.configure();
        Client.init();
        System.out.print("Enter filename: ");
        Scanner reader = new Scanner(System.in);
        String filename = reader.nextLine();
        Client.downloadFile(Client.blockingStub, filename);
    }
}
