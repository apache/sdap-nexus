
package org.nasa.jpl.nexus.ingest.nexussink;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent.NexusTile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;

/**
 * Created by djsilvan on 6/26/17.
 */
public class S3Store implements DataStore {

    private AmazonS3 s3;
    private String bucketName;
    private Logger log = LoggerFactory.getLogger(NexusService.class);

    public S3Store(AmazonS3Client s3client, String bucketName) {
        s3 = s3client;
        this.bucketName = bucketName;
    }

    public void saveData(Collection<NexusTile> nexusTiles) {

        for (NexusTile tile : nexusTiles) {
            String tileId = getTileId(tile);
            byte[] tileData = getTileData(tile);
            Long contentLength = (long) tileData.length;
            InputStream stream = new ByteArrayInputStream(tileData);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(contentLength);

            try {
                s3.putObject(new PutObjectRequest(bucketName, tileId, stream, meta));
            }
            catch (AmazonServiceException ase) {
                log.error("Caught an AmazonServiceException, which means your request made it "
                        + "to Amazon S3, but was rejected with an error response for some reason.");
                throw new DataStoreException(ase);
            }
            catch (AmazonClientException ace) {
                log.error("Caught an AmazonClientException, which means the client encountered "
                        + "a serious internal problem while trying to communicate with S3, "
                        + "such as not being able to access the network.");
                throw new DataStoreException(ace);
            }
        }
    }

    private String getTileId(NexusTile tile) {
        return tile.getTile().getTileId();
    }

    private byte[] getTileData(NexusTile tile) {
        return tile.getTile().toByteArray();
    }
}
