package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.stream.StreamObserver;

public class StreamOutboundWriter implements StreamObserver<Object> {


    private Processor.Observer observer;
    private Processor processor;

    public StreamOutboundWriter(Processor processor) {
        this.observer = processor.getObserver();
        this.processor = processor;
    }

    @Override
    public void onNext(Object o) throws Exception {
        observer.onNext(o);
    }

    @Override
    public void onError(Throwable t) throws Exception {
        onCancel();
    }

    @Override
    public void onComplete() throws Exception {
        observer.onComplete();
    }

    public void onCancel() throws Exception {
        observer.onCancel();
    }
}
