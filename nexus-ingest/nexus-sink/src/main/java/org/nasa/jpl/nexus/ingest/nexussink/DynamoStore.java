
package org.nasa.jpl.nexus.ingest.nexussink;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by djsilvan on 6/26/17.
 */
public class DynamoStore implements DataStore {

    private DynamoDB dynamoDB;
    private String tableName;
    private String primaryKey = "tile_id";
    private Logger log = LoggerFactory.getLogger(NexusService.class);

    public DynamoStore(AmazonDynamoDB dynamoClient, String tableName) {
        dynamoDB = new DynamoDB(dynamoClient);
        this.tableName = tableName;
    }

    public void saveData(Collection<NexusContent.NexusTile> nexusTiles) {

        Table table = dynamoDB.getTable(tableName);

        for (NexusContent.NexusTile tile : nexusTiles) {
            String tileId = getTileId(tile);
            byte[] tileData = getTileData(tile);

            try {
                table.putItem(new Item().withPrimaryKey(primaryKey, tileId).withBinary("data", tileData));
            }
            catch (Exception e) {
                log.error("Unable to add item: " + tileId);
                throw new DataStoreException(e);
            }
        }
    }

    private String getTileId(NexusContent.NexusTile tile) {
        return tile.getTile().getTileId();
    }

    private byte[] getTileData(NexusContent.NexusTile tile) {
        return tile.getTile().toByteArray();
    }
}
