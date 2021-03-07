/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.protocol.tri.GrpcStatus.Code;
import org.apache.dubbo.triple.TripleWrapper;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.dubbo.rpc.protocol.tri.TripleUtil.responseErr;

public class ServerStream extends AbstractStream implements Stream {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerStream.class);
    private static final String TOO_MANY_REQ = "Too many requests";
    private static final String MISSING_REQ = "Missing request";
    private final ChannelHandlerContext ctx;
    private volatile boolean headerSent = false;
    private Processor processor;


    public ServerStream(ChannelHandlerContext ctx) {
        super(ExecutorUtil.setThreadName(null, "DubboPUServerHandler"), ctx);
        this.ctx = ctx;
    }

    public void setProcessor(Processor processor) {
        this.processor = processor;
    }

    @Override
    protected void onSingleMessage(InputStream in) throws Exception {
        processor.sendMessage(in);
    }


    @Override
    public void onError(GrpcStatus status) {
    }

    @Override
    public void write(Object obj, ChannelPromise promise) throws Exception {
        writeHttp2Headers();
        writeHttp2Data(processor.codeResponseMessage(obj));
    }

    protected void writeHttp2Headers() {
        Http2Headers http2Headers = new DefaultHttp2Headers()
            .status(OK.codeAsText())
            .set(HttpHeaderNames.CONTENT_TYPE, TripleConstant.CONTENT_PROTO);
        if (!headerSent) {
            headerSent = true;
            ctx.write(new DefaultHttp2HeadersFrame(http2Headers));
        }
    }

    protected void writeHttp2Data(ByteBuf buf) {
        final DefaultHttp2DataFrame data = new DefaultHttp2DataFrame(buf);
        ctx.write(data);
    }

    protected void writeHttp2Trailers(final Map<String, Object> attachments) throws IOException {
        final Http2Headers trailers = new DefaultHttp2Headers()
            .setInt(TripleConstant.STATUS_KEY, GrpcStatus.Code.OK.code);
        if (attachments != null) {
            convertAttachment(trailers, attachments);
        }
        ctx.writeAndFlush(new DefaultHttp2HeadersFrame(trailers, true));
    }

    public void halfClose() throws Exception {
        if (getData() == null) {
            responseErr(ctx, GrpcStatus.fromCode(GrpcStatus.Code.INTERNAL)
                    .withDescription(MISSING_REQ));
            return;
        }
        ExecutorService executor = processor.getExecutor();
        try {
            executor.execute(processor::complete);
        } catch (RejectedExecutionException e) {
            LOGGER.error("Provider's thread pool is full", e);
            responseErr(ctx, GrpcStatus.fromCode(Code.RESOURCE_EXHAUSTED)
                    .withDescription("Provider's thread pool is full"));
        } catch (Throwable t) {
            LOGGER.error("Provider submit request to thread pool error ", t);
            responseErr(ctx, GrpcStatus.fromCode(Code.INTERNAL)
                    .withCause(t)
                    .withDescription("Provider's error"));
        }
    }

}
