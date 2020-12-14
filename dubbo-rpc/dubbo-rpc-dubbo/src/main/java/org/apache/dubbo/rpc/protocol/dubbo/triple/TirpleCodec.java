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
package org.apache.dubbo.rpc.protocol.dubbo.triple;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.Map;

import com.alibabacloud.hipstershop.cartserviceapi.service.CartServiceComplexTest2Request;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.GrpcCodec;
import org.apache.dubbo.remoting.GrpcPacket;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.protocol.dubbo.ProtoUtil;

import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;

/**
 * Dubbo codec.
 */
public class TirpleCodec implements GrpcCodec {

    @Override
    public Object decode(GrpcPacket packet) throws IOException {
        Map<String, String> metadata = packet.getMetadata();
        String path = metadata.get(":path");
        String[] parts = path.split("/");
        String serviceName = parts[1];
        String methodName = parts[2];

        RpcInvocation res = new RpcInvocation();
        res.setAttachment(PATH_KEY, serviceName);
        res.setServiceName(serviceName);
        res.setMethodName(methodName);
        ServiceRepository repo = ApplicationModel.getServiceRepository();
        MethodDescriptor methodDescriptor = repo.lookupMethod(serviceName, methodName);
        Parameter[] parameters = null;
        if (methodDescriptor == null) {
            parameters = new Parameter[0];
        } else {
            parameters = methodDescriptor.getMethod().getParameters();
        }
        Class<?> requestClass = buildArgumentClass(parameters);
        res.setParameterTypes(new Class[]{requestClass});

        res.setTargetServiceUniqueName("targetServiceName");

        final Parser<?> parser = ProtoUtil.getParser(requestClass);
        Object result;
        try {
            byte[] pre = new byte[5];
            packet.getInputStream().read(pre);
            result = parser.parseFrom(packet.getInputStream());
            res.setArguments(new Object[]{result});
            System.out.println("result:" + result);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object message) throws IOException {

    }

    private Class<?> buildArgumentClass(Parameter[] parameters) {
        for (Parameter parameter : parameters) {
            System.out.println("parameter: "+parameter);
        }
        return CartServiceComplexTest2Request.class;
    }
}
