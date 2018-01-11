
package org.nasa.ingest.tcpshell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.SocketUtils;

/**
 * Created by greguska on 3/21/16.
 */
public class PortResolver{

    private final static Logger log = LoggerFactory.getLogger(PortResolver.class);

    public static Integer resolvePort(Integer port){
        if(port != null){
            return port;
        }else {
            return SocketUtils.findAvailableTcpPort();
        }
    }
}
