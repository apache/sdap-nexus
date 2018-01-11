
/**
 * Created by greguska on 3/29/16.
 */

@Grapes([
    @Grab(group = 'org.nasa.jpl.nexus', module = 'nexus-messages', version = '1.1.0.RELEASE')
])

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent

if (!(payload instanceof byte[])){
    throw new RuntimeException("Can't handle messages that are not byte[]. Got payload of type ${payload.class}")
}
thesalt = binding.variables.get("salt")


def tileBuilder = NexusContent.NexusTile.newBuilder().mergeFrom(payload)
def summaryBuilder = tileBuilder.getSummaryBuilder()

def granule = summaryBuilder.hasGranule()?summaryBuilder.granule:"${headers['absolutefilepath']}".split(File.separator)[-1]
def originalSpec = summaryBuilder.hasSectionSpec()?summaryBuilder.sectionSpec:"${headers['spec']}"


def tileId = UUID.nameUUIDFromBytes("${granule[0..-4]}$originalSpec${thesalt==null?'':thesalt}".toString().bytes).toString()

summaryBuilder.setGranule(granule)
summaryBuilder.setTileId(tileId)
summaryBuilder.setSectionSpec(originalSpec)

tileBuilder.getTileBuilder().setTileId(tileId)

return tileBuilder.build().toByteArray()

