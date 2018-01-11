
package org.nasa.jpl.nexus.ingest.nexussink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by djsilvan on 8/11/17.
 */
public class DataStoreException extends RuntimeException {

    private Logger log = LoggerFactory.getLogger(NexusService.class);

    public DataStoreException() {
        log.error("Error: DataStore Exception");
    }

    public DataStoreException(Exception e) {
        log.error("Error: " + e.getMessage());
    }
}
