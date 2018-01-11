
package org.nasa.jpl.nexus.ingest.nexussink;

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent;

import java.util.Collection;

/**
 * Created by djsilvan on 6/26/17.
 */
public interface MetadataStore {

    public void saveMetadata(Collection<NexusContent.NexusTile> nexusTiles);
}
