package org.apache.dubbo.rpc.protocol.tri;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import io.netty.buffer.ByteBuf;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.model.MethodDescriptor;

public interface Processor {

    public abstract void request(int numMessages);

    public abstract void sendHeaders();

    public abstract void complete();

    public abstract void sendMessage(InputStream in) throws Exception;

    Object[] decodeRequestMessage(InputStream in);

    ByteBuf codeResponseMessage(Object value);

    Invocation buildInvocation();

    Observer<Object> getObserver();

    ExecutorService getExecutor();

    public abstract static class Observer<Object> {

        public void onCancel() throws Exception {}

        public void onComplete() throws Exception {}

        public void onNext(java.lang.Object var1) throws Exception {}

        public void onError(Throwable var1) throws Exception {}

        public void onHalfClose() throws Exception {}
    }
}
