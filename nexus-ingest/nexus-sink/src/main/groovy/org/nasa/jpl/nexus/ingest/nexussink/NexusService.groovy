/*****************************************************************************
 * Copyright (c) 2016 Jet Propulsion Laboratory,
 * California Institute of Technology.  All rights reserved
 *****************************************************************************/
package org.nasa.jpl.nexus.ingest.nexussink

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent

/**
 * Created by greguska on 4/4/16.
 */
class NexusService {

    private MetadataStore metadataStore
    private DataStore dataStore

    public NexusService(MetadataStore metadataStore, DataStore dataStore) {
        this.metadataStore = metadataStore
        this.dataStore = dataStore
    }

    def saveToNexus(Collection<NexusContent.NexusTile> nexusTiles) {
        metadataStore.saveMetadata(nexusTiles)
        dataStore.saveData(nexusTiles)
    }

    public static void main(String... args) {

    }
}
