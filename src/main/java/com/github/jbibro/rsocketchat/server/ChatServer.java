package com.github.jbibro.rsocketchat.server;

import java.io.IOException;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public final class ChatServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        Connection nc = Nats.connect();
        RSocketFactory.receive()
            .acceptor((setup, sendingSocket) -> {
                AbstractRSocket rSocket = new AbstractRSocket() {
                    @Override
                    public Mono<Void> fireAndForget(Payload payload) {
                        nc.publish("chat", payload.getData().array());
                        return Mono.empty();
                    }

                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        return Flux.<String>push(sink ->
                            nc.createDispatcher(msg ->
                                sink.next(new String(msg.getData(), UTF_8))).subscribe("chat")
                        )
                            .map(DefaultPayload::create);
                    }
                };
                return Mono.just(rSocket);
            })
            .transport(TcpServerTransport.create(8081))
            .start()
            .block()
            .onClose()
            .block();
    }
}
