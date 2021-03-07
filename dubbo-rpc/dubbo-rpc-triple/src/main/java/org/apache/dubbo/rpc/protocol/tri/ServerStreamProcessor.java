package org.apache.dubbo.rpc.protocol.tri;

import java.io.InputStream;
import java.util.Map;
import io.netty.channel.ChannelHandlerContext;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.protocol.tri.GrpcStatus.Code;

import static org.apache.dubbo.rpc.protocol.tri.TripleUtil.responseErr;

public class ServerStreamProcessor extends AbstractServerProcessor {

    private StreamObserver<Object> respObserver;
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerStreamProcessor.class);

    public ServerStreamProcessor(ServerStream serverStream, Invoker<?> invoker,
        ServiceDescriptor serviceDescriptor, MethodDescriptor md, ChannelHandlerContext ctx) {
        super(serverStream, invoker, serviceDescriptor, md, ctx);
    }

    public void setRespObserver(StreamObserver<Object> respObserver) {
        this.respObserver = respObserver;
    }

    @Override
    public void sendHeaders() {

    }

    @Override
    public void complete()  {
        try {
            this.respObserver.onComplete();
        } catch (Exception t) {
            LOGGER.warn("Exception processing triple message", t);
            responseErr(ctx, GrpcStatus.fromCode(Code.INTERNAL).withDescription("Decode request failed:" + t.getMessage()));
            return;
        }
    }

    @Override
    public void sendMessage(InputStream in) throws Exception {
        // TODO do not support multiple arguments for stream
        final Object[] resp = decodeRequestMessage(in);
        if (resp.length >= 1) {
            return;
        }
        respObserver.onNext(resp);
    }


    public Observer<Object> getObserver() {
        return new StreamServerObserver();
    }


    private final class StreamServerObserver  extends Observer<Object>  {

        @Override
        public void onNext(Object obj) throws Exception {
            serverStream.write(obj, null);
        }

        @Override
        public void onComplete() throws Exception {
            serverStream.writeHttp2Trailers(null);
        }

        @Override
        public void onCancel() throws Exception {

        }
    }












}
