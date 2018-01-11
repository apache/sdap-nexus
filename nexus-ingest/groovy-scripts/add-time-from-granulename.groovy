
/**
 * Created by greguska on 3/29/16.
 */

@Grapes([
    @Grab(group = 'org.nasa.jpl.nexus', module = 'nexus-messages', version = '1.1.0.RELEASE')
])

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent


if (regex == null ){
    throw new RuntimeException("This script requires a regex to use for matching against the granulename.")
}
if (dateformat == null ){
    throw new RuntimeException("This script requires a date format to parse the date extracted using the regex.")
}
if (!(payload instanceof byte[])){
    throw new RuntimeException("Can't handle messages that are not byte[]. Got payload of type ${payload.class}")
}

def pattern = ~"$regex"
def tileBuilder = NexusContent.NexusTile.newBuilder().mergeFrom(payload)

switch(tileBuilder.tile.tileTypeCase){
    case NexusContent.TileData.TileTypeCase.GRID_TILE:
        def gridtilebuilder = tileBuilder.tileBuilder.gridTileBuilder
        def tilesummary = tileBuilder.summaryBuilder

        def granulename = tilesummary.granule

        def matches = (granulename =~ pattern)

        if (!matches.hasGroup()){
            throw new RuntimeException("regex did not return any groups.")
        }
        if (1 != matches.size()){
            throw new RuntimeException("regex did not return *one* group.")
        }
        if (2 != matches[0].size()){
            throw new RuntimeException("group does not contain match.")
        }

        def datestring = matches[0][1]

        def time = Date.parse(dateformat, datestring.toString(), TimeZone.getTimeZone("UTC"))


        def secondsSinceEpoch = (time.time / 1000).toLong()

        gridtilebuilder.setTime(secondsSinceEpoch)
        tilesummary.statsBuilder.setMinTime(secondsSinceEpoch)
        tilesummary.statsBuilder.setMaxTime(secondsSinceEpoch)
        break
    default:
        throw new RuntimeException("Can only handle GridTile at this time.")
}

return tileBuilder.build().toByteArray()
