package com.github.jbibro.rsocketchat.server;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

@Slf4j
public final class ChatServer {
    public static void main(String[] args) {
        ReplayProcessor<String> replyProcessor = ReplayProcessor.create();
        Flux<String> flux = replyProcessor.replay(10).autoConnect();
        FluxSink<String> sink = replyProcessor.sink();

        RSocketFactory.receive()
            .acceptor((setup, sendingSocket) -> {
                AbstractRSocket rSocket = new AbstractRSocket() {
                    @Override
                    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        Flux
                            .from(payloads)
                            .map(Payload::getDataUtf8)
                            .subscribe(sink::next);
                        return flux.map(DefaultPayload::create);
                    }
                };
                return Mono.just(rSocket);
            })
            .transport(TcpServerTransport.create("0.0.0.0", 8080))
            .start()
            .block()
            .onClose()
            .block();
    }
}
