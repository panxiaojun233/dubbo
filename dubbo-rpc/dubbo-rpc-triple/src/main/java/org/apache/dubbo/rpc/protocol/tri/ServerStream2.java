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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ServiceRepository;

import java.io.InputStream;

public abstract class ServerStream2 extends AbstractStream2 implements Stream {
    protected static final String MISSING_REQ = "Missing request";
    private final ProviderModel providerModel;
    private MethodDescriptor md;
    private Invoker<?> invoker;
    private Metadata metadata;
    protected ServerStream2(URL url) {
        super(url);
        this.providerModel = lookupProviderModel();
    }

    public static ServerStream2 unary(URL url) {
        return new UnaryServerStream2(url);

    }

    public static ServerStream2 stream(URL url) {

    }

    public ProviderModel getProviderModel() {
        return providerModel;
    }

    private ProviderModel lookupProviderModel() {
        ServiceRepository repo = ApplicationModel.getServiceRepository();
        final ProviderModel model = repo.lookupExportedService(getUrl().getServiceKey());
        if (model != null) {
            ClassLoadUtil.switchContextLoader(model.getServiceInterfaceClass().getClassLoader());
        }
        return model;
    }

    public ServerStream2 method(MethodDescriptor md) {
        this.md = md;
        return this;
    }

    public ServerStream2 invoker(Invoker<?> invoker) {
        this.invoker = invoker;
        return this;
    }

    @Override
    public void onMetadata(Metadata metadata, OperationHandler handler) {
        this.metadata = metadata;
    }

    @Override
    public void onData(InputStream in, OperationHandler handler) {
        if (in == null) {
            handler.operationDone(OperationResult.FAILURE, GrpcStatus.fromCode(GrpcStatus.Code.INTERNAL)
                    .withDescription(MISSING_REQ).asException());
            return;
        }
        appendData(in, handler);
    }

    protected abstract void appendData(InputStream in, OperationHandler handler);

    @Override
    public void onComplete(OperationHandler handler) {
        getSubscriber().onCompleted();
    }

    @Override
    public void onError(Throwable t, OperationHandler handler) {

    }
}
