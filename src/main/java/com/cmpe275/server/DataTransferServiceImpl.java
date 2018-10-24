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


import com.cmpe275.generated.Chat;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

import com.cmpe275.generated.DataTransferServiceGrpc.DataTransferServiceImplBase;
import io.grpc.stub.StreamObserver;

public class DataTransferServiceImpl extends  DataTransferServiceImplBase{
    private Server svr;

    public static void main(String[] args) throws Exception {
        // TODO check args!
        try {
            RingServer.configure();

            final DataTransferServiceImpl impl = new DataTransferServiceImpl();
            impl.start();
            impl.blockUntilShutdown();

        } catch (IOException e) {
            // TODO better error message
            e.printStackTrace();
        }
    }

    private void start() throws Exception {
        svr = ServerBuilder.forPort(RingServer.getInstance().getServerPort()).addService(new DataTransferServiceImpl())
                .build();

        System.out.println("-- starting server");
        svr.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                DataTransferServiceImpl.this.stop();
            }
        });
    }

    protected void stop() {
        svr.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        svr.awaitTermination();
    }

    @Override
    public void send(Chat.Message request, StreamObserver<Chat.Ack> responseObserver) {
        Chat.Message.Builder builder = Chat.Message.newBuilder();


    }

    @Override
    public void ping(Chat.User request, StreamObserver<Chat.Pong> responseObserver) {
        //not clear what to do
    }
}
