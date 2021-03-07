package org.apache.dubbo.rpc.protocol.tri;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.protocol.tri.GrpcStatus.Code;

import static org.apache.dubbo.rpc.protocol.tri.TripleUtil.responseErr;

public class ServerUnaryProcessor extends AbstractServerProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerUnaryProcessor.class);

    public ServerUnaryProcessor(ServerStream serverStream, Invoker<?> invoker,
        ServiceDescriptor serviceDescriptor, MethodDescriptor md,
        ChannelHandlerContext ctx) {
        super(serverStream, invoker, serviceDescriptor, md, ctx);
    }

    @Override
    public void sendHeaders() {

    }

    @Override
    public void sendMessage(InputStream in) {
        // ignore
    }

    @Override
    public Observer<Object> getObserver() {
        return new UnaryServerObserver();
    }


    private final class UnaryServerObserver extends Processor.Observer<Object> {

        @Override
        public void onHalfClose() throws Exception {
            serverStream.halfClose();
        }
    }

    @Override
    public void complete() {

        Invocation invocation;
        try {
            invocation = buildInvocation();
        } catch (Throwable t) {
            LOGGER.warn("Exception processing triple message", t);
            responseErr(ctx, GrpcStatus.fromCode(Code.INTERNAL).withDescription("Decode request failed:" + t.getMessage()));
            return;
        }
        if (invocation == null) {
            return;
        }

        final Result result = invoker.invoke(invocation);
        CompletionStage<Object> future = result.thenApply(Function.identity());

        BiConsumer<Object, Throwable> onComplete = (appResult, t) -> {
            try {
                if (t != null) {
                    if (t instanceof TimeoutException) {
                        responseErr(ctx, GrpcStatus.fromCode(Code.DEADLINE_EXCEEDED).withCause(t));
                    } else {
                        responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.UNKNOWN).withCause(t));
                    }
                    return;
                }
                AppResponse response = (AppResponse) appResult;
                if (response.hasException()) {
                    final Throwable exception = response.getException();
                    if (exception instanceof TripleRpcException) {
                        responseErr(ctx, ((TripleRpcException) exception).getStatus());
                    } else {
                        responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.UNKNOWN)
                            .withCause(exception));
                    }
                    return;
                }
                ClassLoader tccl = Thread.currentThread().getContextClassLoader();
                final ByteBuf buf;
                try {
                    ClassLoadUtil.switchContextLoader(providerModel.getServiceInterfaceClass().getClassLoader());
                    buf = codeResponseMessage(response.getValue());
                } finally {
                    ClassLoadUtil.switchContextLoader(tccl);
                }

                serverStream.writeHttp2Headers();
                serverStream.writeHttp2Data(buf);
                serverStream.writeHttp2Trailers(response.getObjectAttachments());
            } catch (Throwable e) {
                LOGGER.warn("Exception processing triple message", e);
                if (e instanceof TripleRpcException) {
                    responseErr(ctx, ((TripleRpcException) e).getStatus());
                } else {
                    responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.UNKNOWN)
                        .withDescription("Exception occurred in provider's execution:" + e.getMessage())
                        .withCause(e));
                }
            }
        };

        future.whenComplete(onComplete);
    }
}
