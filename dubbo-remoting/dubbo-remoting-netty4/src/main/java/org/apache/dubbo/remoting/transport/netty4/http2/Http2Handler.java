/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.dubbo.remoting.transport.netty4.http2;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.remoting.GrpcCodec;
import org.apache.dubbo.remoting.transport.netty4.grpc.GrpcElf;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * A simple handler that responds with the message "Hello World!".
 */
public final class Http2Handler extends Http2ConnectionHandler implements Http2FrameListener {

    protected Http2Connection.PropertyKey streamKey;

    static final ByteBuf RESPONSE_BYTES = unreleasableBuffer(copiedBuffer("Hello World", CharsetUtil.UTF_8));

    Http2Handler(Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder,
                           Http2Settings initialSettings) {
        super(decoder, encoder, initialSettings);
        this.streamKey = encoder.connection().newKey();
    }

    private static Http2Headers http1HeadersToHttp2Headers(FullHttpRequest request) {
        CharSequence host = request.headers().get(HttpHeaderNames.HOST);
        Http2Headers http2Headers = new DefaultHttp2Headers()
                .method(HttpMethod.GET.asciiName())
                .path(request.uri())
                .scheme(HttpScheme.HTTP.name());
        if (host != null) {
            http2Headers.authority(host);
        }
        return http2Headers;
    }

    /**
     * Handles the cleartext HTTP upgrade event. If an upgrade occurred, sends a simple response via HTTP/2
     * on stream 1 (the stream specifically reserved for cleartext HTTP upgrade).
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
            HttpServerUpgradeHandler.UpgradeEvent upgradeEvent =
                    (HttpServerUpgradeHandler.UpgradeEvent) evt;
            onHeadersRead(ctx, 1, http1HeadersToHttp2Headers(upgradeEvent.upgradeRequest()), 0 , true);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        cause.printStackTrace();
        ctx.close();
    }

    /**
     * Sends a "Hello World" DATA frame to the client.
     */
    private void sendResponse(ChannelHandlerContext ctx, int streamId, ByteBuf payload) {
        // Send a frame for the response status
        Http2Headers headers = new DefaultHttp2Headers().status(OK.codeAsText());
        encoder().writeHeaders(ctx, streamId, headers, 0, false, ctx.newPromise());
        encoder().writeData(ctx, streamId, payload, 0, true, ctx.newPromise());

        // no need to call flush as channelReadComplete(...) will take care of it.
    }

    @Override
    public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream) {

        Channel channel = ctx.channel();
        Http2Stream stream = connection().stream(streamId);
        StreamPayload payload = stream == null ? null : (StreamPayload) stream.getProperty(streamKey);
        Object result = null;
        if (payload == null || payload.streamId() != streamId) {
            // never happen ？
            //throw new RemotingException((InetSocketAddress) channel.localAddress(), (InetSocketAddress) channel.remoteAddress(),
            //    "received remote data from streamId:" + streamId + ", but not found payload.");
            System.out.println("received remote data from streamId:" + streamId + ", but not found payload.");
        }

        if (payload.data() == null) {
            payload.data(ctx.alloc().buffer());
            ReferenceCountUtil.retain(payload.data());
        }

        payload.endOfStream(endOfStream).data().writeBytes(data);

        System.out.println("received data streamId:" + streamId + " length:" + data.readableBytes() + " endOfStream:" + endOfStream);

        if (endOfStream) {
            if (endOfStream) {
                GrpcCodec codec = ExtensionLoader.getExtensionLoader(GrpcCodec.class).getExtension("triple");
                try {
                    Object res = codec.decode(payload);
                    ctx.fireChannelRead(res);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //sendResponse(ctx, streamId, data.retain());
            }
        }

        int processed = data.readableBytes() + padding;
        //if (endOfStream) {
        //    sendResponse(ctx, streamId, data.retain());
        //}
        return processed;
    }

    @Override
    public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
                              Http2Headers headers, int padding, boolean endOfStream) {
        if (headers.path() == null) {
            System.out.println("Expected path but is missing");
            return;
        }

        final String path = headers.path().toString();
        if (path.charAt(0) != '/') {
            System.out.println("Expected path but is missing1");
            return;
        }

        final CharSequence contentType = HttpUtil.getMimeType(headers.get(HttpHeaderNames.CONTENT_TYPE));
        if (contentType == null) {
            System.out.println("Expected path but is missing2");
            return;
        }

        if (!GrpcElf.isGrpcContentType(contentType)) {
            System.out.println("Expected path but is missing3");
            return;
        }

        if (!HttpMethod.POST.asciiName().equals(headers.method())) {
            System.out.println("Expected path but is missing4");
            return;
        }

        String marshaller;
        if (AsciiString.contentEquals(contentType, GrpcElf.APPLICATION_GRPC) || AsciiString.contentEquals(contentType, GrpcElf.GRPC_PROTO)) {
            marshaller = "protobuf";
        } else if (AsciiString.contentEquals(contentType, GrpcElf.GRPC_JSON)) {
            marshaller = "protobuf-json";
        } else {
            System.out.println("Expected path but is missing5");
            return;
        }

        Http2Stream http2Stream = connection().stream(streamId);
        StreamPayload payload = new StreamPayload(http2Stream, headers, streamKey);
        payload.streamId(streamId).endOfStream(endOfStream);
        http2Stream.setProperty(streamKey, payload);

        if (endOfStream) {
            ByteBuf content = ctx.alloc().buffer();
            content.writeBytes(RESPONSE_BYTES.duplicate());
            ByteBufUtil.writeAscii(content, " - via HTTP/2");
            sendResponse(ctx, streamId, content);
        }
    }

    @Override
    public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
                              short weight, boolean exclusive, int padding, boolean endOfStream) {
        onHeadersRead(ctx, streamId, headers, padding, endOfStream);
    }

    @Override
    public void onPriorityRead(ChannelHandlerContext ctx, int streamId, int streamDependency,
                               short weight, boolean exclusive) {
    }

    @Override
    public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
    }

    @Override
    public void onSettingsAckRead(ChannelHandlerContext ctx) {
    }

    @Override
    public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
    }

    @Override
    public void onPingRead(ChannelHandlerContext ctx, long data) {
    }

    @Override
    public void onPingAckRead(ChannelHandlerContext ctx, long data) {
    }

    @Override
    public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId,
                                  Http2Headers headers, int padding) {
    }

    @Override
    public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
    }

    @Override
    public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
    }

    @Override
    public void onUnknownFrame(ChannelHandlerContext ctx, byte frameType, int streamId,
                               Http2Flags flags, ByteBuf payload) {
    }
}
