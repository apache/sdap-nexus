
package org.nasa.jpl.nexus.ingest.nexussink

import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.SolrInputField
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent
import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent.TileSummary
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.env.Environment
import org.springframework.data.solr.core.SolrOperations

import javax.annotation.Resource
import java.text.SimpleDateFormat

/**
 * Created by djsilvan on 6/27/17.
 */
class SolrStore implements MetadataStore {

    private Environment environment
    private SolrOperations solr

    private Logger log = LoggerFactory.getLogger(SolrStore.class)

    //TODO This will be refactored at some point to be dynamic per-message. Or maybe per-group.
    private String tableName = "sea_surface_temp"

    private static final def iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    static {
        iso.setTimeZone(TimeZone.getTimeZone("UTC"))
    }

    public SolrStore(SolrOperations solr) {
        this.solr = solr
    }

    @Resource
    void setEnvironment(Environment environment) {
        this.environment = environment
    }

    void saveMetadata(Collection<NexusContent.NexusTile> nexusTiles) {

        def solrdocs = nexusTiles.collect { nexusTile -> getSolrDocFromTileSummary(nexusTile.summary) }
        solr.saveDocuments(solrdocs, environment.getProperty("solrCommitWithin", Integer.class, 1000))
    }

    def getSolrDocFromTileSummary(TileSummary summary) {

        def bbox = summary.getBbox()
        def stats = summary.getStats()

        def startCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        startCal.setTime(new Date(stats.minTime * 1000))
        def endCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        endCal.setTime(new Date(stats.maxTime * 1000))

        def minTime = iso.format(startCal.getTime())
        def maxTime = iso.format(endCal.getTime())

        def geo = determineGeo(summary)

        def doc = [
                "table_s"         : tableName,
                "geo"             : geo,
                "id"              : "$summary.tileId".toString(),
                "solr_id_s"       : "${summary.datasetName}!${summary.tileId}".toString(),
                "dataset_id_s"    : "$summary.datasetUuid".toString(),
                "sectionSpec_s"   : "$summary.sectionSpec".toString(),
                "dataset_s"       : "$summary.datasetName".toString(),
                "granule_s"       : "$summary.granule".toString(),
                "tile_var_name_s" : "$summary.dataVarName".toString(),
                "tile_min_lon"    : bbox.lonMin,
                "tile_max_lon"    : bbox.lonMax,
                "tile_min_lat"    : bbox.latMin,
                "tile_max_lat"    : bbox.latMax,
                "tile_min_time_dt": minTime,
                "tile_max_time_dt": maxTime,
                "tile_min_val_d"  : stats.min,
                "tile_max_val_d"  : stats.max,
                "tile_avg_val_d"  : stats.mean,
                "tile_count_i"    : stats.count
        ]

        summary.globalAttributesList.forEach { attribute ->
            doc["${attribute.name}"] = attribute.valuesCount == 1 ? attribute.getValues(0) : attribute.getValuesList().toList()
        }

        def solrdoc = toSolrInputDocument(doc)
        return solrdoc
    }

    private determineGeo(def summary) {
        //Solr cannot index a POLYGON where all corners are the same point or when there are only 2 distinct points (line).
        //Solr is configured for a specific precision so we need to round to that precision before checking equality.
        def geoPrecision = environment.getProperty("solrGeoPrecision", Integer.class, 3)
        def latMin = summary.bbox.latMin.round(geoPrecision)
        def latMax = summary.bbox.latMax.round(geoPrecision)
        def lonMin = summary.bbox.lonMin.round(geoPrecision)
        def lonMax = summary.bbox.lonMax.round(geoPrecision)
        def geo
        //If lat min = lat max and lon min = lon max, index the 'geo' bounding box as a POINT instead of a POLYGON
        if (latMin == latMax && lonMin == lonMax) {
            geo = "POINT(${lonMin} ${latMin})"
            log.debug("${summary.tileId}\t${summary.granule}[${summary.sectionSpec}] geo=$geo")
        }
        //If lat min = lat max but lon min != lon max, then we essentially have a line.
        else if (latMin == latMax) {
            geo = "LINESTRING (${lonMin} ${latMin}, ${lonMax} ${latMin})"
            log.debug("${summary.tileId}\t${summary.granule}[${summary.sectionSpec}] geo=$geo")
        }
        //Same if lon min = lon max but lat min != lat max
        else if (lonMin == lonMax) {
            geo = "LINESTRING (${lonMin} ${latMin}, ${lonMin} ${latMax})"
            log.debug("${summary.tileId}\t${summary.granule}[${summary.sectionSpec}] geo=$geo")
        }
        //All other cases should use POLYGON
        else {
            geo = "POLYGON((" +
                    "${lonMin} ${latMin}, " +
                    "${lonMax} ${latMin}, " +
                    "${lonMax} ${latMax}, " +
                    "${lonMin} ${latMax}, " +
                    "${lonMin} ${latMin}))"
        }

        return geo
    }

    def toSolrInputDocument(Map<String, Object> doc) {
        def solrDoc = new SolrInputDocument()
        solrDoc.putAll(doc.collectEntries { String key, Object value ->
            def field = new SolrInputField(key)
            field.setValue(value, 1.0f)
            [(key): field]
        })
        return solrDoc
    }
}
