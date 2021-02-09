package org.apache.dubbo.rpc.protocol.tri;

import io.netty.channel.ChannelHandlerContext;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public class ResponseObserverProcessor implements Subscriber<Object> {

    private ChannelHandlerContext ctx;
    private volatile Subscriber<Object> subscriber;
    private ServerStream stream;
    public ResponseObserverProcessor(ChannelHandlerContext ctx, ServerStream stream, Subscriber<Object> subscriber) {
        this.stream = stream;
        this.ctx = ctx;
        this.subscriber = subscriber;
    }

    public ServerStream getStream() {
        return stream;
    }

    @Override
    public void onSubscribe(Subscription subscription) {

    }

    @Override
    public void onNext(Object o) {
        subscriber.onNext(o);
    }

    @Override
    public void onError(Throwable throwable) {
        subscriber.onError(throwable);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }


}
