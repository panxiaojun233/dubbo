package org.apache.dubbo.rpc.protocol.tri;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.MultipleSerialization;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.config.Constants;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.protocol.tri.GrpcStatus.Code;
import org.apache.dubbo.triple.TripleWrapper;

import static org.apache.dubbo.rpc.protocol.tri.TripleUtil.responseErr;

public abstract class AbstractServerProcessor implements Processor {
    protected final ServerStream serverStream;
    protected final ChannelHandlerContext ctx;
    protected final ServiceDescriptor serviceDescriptor;
    protected final MethodDescriptor md;
    protected final URL url;
    protected final ProviderModel providerModel;
    protected final Invoker<?> invoker;
    private static final ExecutorRepository EXECUTOR_REPOSITORY =
        ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();

    private String serializeType;
    private MultipleSerialization multipleSerialization;

    public AbstractServerProcessor(ServerStream serverStream, Invoker<?> invoker, ServiceDescriptor serviceDescriptor, MethodDescriptor md, ChannelHandlerContext ctx) {
        this.serverStream = serverStream;
        this.md = md;
        this.invoker = invoker;
        this.serviceDescriptor = serviceDescriptor;
        this.ctx = ctx;
        this.url = invoker.getUrl();
        ServiceRepository repo = ApplicationModel.getServiceRepository();
        this.providerModel = repo.lookupExportedService(url.getServiceKey());
    }

    @Override
    public void request(int numMessages) {
        serverStream.request(numMessages);
    }

    @Override
    public Object[] decodeRequestMessage(InputStream is) {

        if (md.isNeedWrap()) {
            final TripleWrapper.TripleRequestWrapper req = TripleUtil.unpack(is, TripleWrapper.TripleRequestWrapper.class);
            this.serializeType = req.getSerializeType();
            String[] paramTypes = req.getArgTypesList().toArray(new String[req.getArgsCount()]);
            if (!Arrays.equals(this.md.getCompatibleParamSignatures(), paramTypes)) {
                //todo error
            }
            final Object[] arguments = TripleUtil.unwrapReq(url, req, multipleSerialization);
            return arguments;
        } else {
            final Object req = TripleUtil.unpack(is, md.getParameterClasses()[0]);
            return new Object[]{req};
        }
    }

    @Override
    public ByteBuf codeResponseMessage(Object value) {
        final Message message;
        if (md.isNeedWrap()) {
            message = TripleUtil.wrapResp(url, serializeType, value, md, multipleSerialization);
        } else {
            message = (Message) value;
        }
        return TripleUtil.pack(ctx, message);
    }

    @Override
    public ExecutorService getExecutor() {
        ExecutorService executor = null;
        if (providerModel != null) {
            executor = (ExecutorService) providerModel.getServiceMetadata().getAttribute(CommonConstants.THREADPOOL_KEY);
        }
        if (executor == null) {
            executor = EXECUTOR_REPOSITORY.getExecutor(url);
        }
        if (executor == null) {
            executor = EXECUTOR_REPOSITORY.createExecutorIfAbsent(url);
        }

        return executor;
    }

    protected void loadFromURL(URL url) {
        final String value = url.getParameter(Constants.MULTI_SERIALIZATION_KEY, "default");
        this.multipleSerialization = ExtensionLoader.getExtensionLoader(MultipleSerialization.class).getExtension(value);
    }

    // todo
    public Invocation getInvocation() {
        RpcInvocation inv = new RpcInvocation();
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();

        if (md.isNeedWrap()) {
            loadFromURL(url);
        }

        inv.setMethodName(md.getMethodName());
        inv.setServiceName(serviceDescriptor.getServiceName());
        inv.setTargetServiceUniqueName(url.getServiceKey());
        if (md.isStream()) {

            inv.setParameterTypes(new Class[] {StreamObserver.class});
            inv.setReturnTypes(new Class[] {StreamObserver.class});
            inv.setArguments(new Object[]{new StreamOutboundWriter(this)});
        } else {
            inv.setParameterTypes(md.getParameterClasses());
            inv.setReturnTypes(md.getReturnTypes());
            InputStream is = serverStream.pollData();
            try {
                if (providerModel != null) {
                    ClassLoadUtil.switchContextLoader(providerModel.getServiceInterfaceClass().getClassLoader());
                }

                inv.setArguments(decodeRequestMessage(is));
            } finally {
                ClassLoadUtil.switchContextLoader(tccl);
            }
        }
        final Map<String, Object> attachments = serverStream.parseHeadersToMap(serverStream.getHeaders());
        inv.setObjectAttachments(attachments);
        return inv;
    }

    @Override
    public Invocation buildInvocation() {
        final List<MethodDescriptor> methods = serviceDescriptor.getMethods(md.getMethodName());
        if (methods == null || methods.isEmpty()) {
            responseErr(ctx, GrpcStatus.fromCode(Code.UNIMPLEMENTED)
                .withDescription("Method not found:" + md + " of service:" + serviceDescriptor.getServiceName()));
            return null;
        }

        if (this.md == null) {
            responseErr(ctx, GrpcStatus.fromCode(Code.UNIMPLEMENTED)
                .withDescription("Method not found:" + md.getMethodName() +
                    " args:" + Arrays.toString(md.getParameterClasses()) + " of service:" + serviceDescriptor.getServiceName()));
            return null;
        }

        return getInvocation();
    }

}
