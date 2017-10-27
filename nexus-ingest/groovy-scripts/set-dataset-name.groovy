/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
/**
 * Created by greguska on 3/29/16.
 */

@Grapes([
    @Grab(group = 'org.nasa.jpl.nexus', module = 'nexus-messages', version = '1.0.0.RELEASE')
])

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent

if (!(payload instanceof byte[])){
    throw new RuntimeException("Can't handle messages that are not byte[]. Got payload of type ${payload.class}")
}

def tileBuilder = NexusContent.NexusTile.newBuilder().mergeFrom(payload)


assert datasetname != null : "This script requires a variable called datasetname."

def summaryBuilder = tileBuilder.getSummaryBuilder()
summaryBuilder.setDatasetName(datasetname)

return tileBuilder.build().toByteArray()

