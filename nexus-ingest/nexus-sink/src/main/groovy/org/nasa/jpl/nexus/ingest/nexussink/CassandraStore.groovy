
package org.nasa.jpl.nexus.ingest.nexussink

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent.NexusTile
import org.springframework.data.cassandra.core.CassandraOperations
import java.nio.ByteBuffer

/**
 * Created by djsilvan on 6/27/17.
 */
class CassandraStore implements DataStore {

    private CassandraOperations cassandraTemplate

    //TODO This will be refactored at some point to be dynamic per-message. Or maybe per-group.
    private String tableName = "sea_surface_temp"

    public CassandraStore(CassandraOperations cassandraTemplate) {
        this.cassandraTemplate = cassandraTemplate
    }

    @Override
    void saveData(Collection<NexusTile> nexusTiles) {

        def query = "insert into ${tableName} (tile_id, tile_blob) VALUES (?, ?)"
        cassandraTemplate.ingest(query, nexusTiles.collect { nexusTile -> getCassandraRowFromTileData(nexusTile.tile) })
    }

    def getCassandraRowFromTileData(NexusContent.TileData tile) {

        def tileId = UUID.fromString(tile.tileId)
        def row = [tileId, ByteBuffer.wrap(tile.toByteArray())]
        return row
    }

}