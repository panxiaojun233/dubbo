package org.apache.dubbo.remoting;

import java.io.InputStream;
import java.util.Map;

/**
 * convert Object from/to InputStream
 */

public interface GrpcPacket {

    Map<String, String> getMetadata();

    InputStream getInputStream();
}
