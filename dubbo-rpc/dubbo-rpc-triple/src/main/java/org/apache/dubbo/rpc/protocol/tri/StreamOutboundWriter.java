package org.apache.dubbo.rpc.protocol.tri;


import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;


public class StreamOutboundWriter implements Subscriber<Object> {

    private Subscription subscription;
    private ServerStream stream;
    private final AtomicBoolean canceled = new AtomicBoolean();

    public StreamOutboundWriter(ServerStream stream) {
        this.stream = stream;
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        s.request(1);
    }

    @Override
    public void onNext(Object o) {
        stream.writeObjectOut(o);
    }

    @Override
    public void onError(Throwable t) {
        doCancel();
    }

    @Override
    public void onComplete() {
        stream.onComplete();
    }

    public void doCancel() {
        if (canceled.compareAndSet(false, true)) {
            if (subscription != null) {
                subscription.cancel();
            }
            stream.onComplete();
        }
    }
}
