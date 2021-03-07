package org.apache.dubbo.common.stream;

public interface StreamObserver<T> {
    void onNext(T var1) throws Exception;

    void onError(Throwable var1) throws Exception;

    void onComplete() throws Exception;
}
