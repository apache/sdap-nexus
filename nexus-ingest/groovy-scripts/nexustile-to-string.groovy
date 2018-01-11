/*****************************************************************************
 * Copyright (c) 2016 Jet Propulsion Laboratory,
 * California Institute of Technology.  All rights reserved
 *****************************************************************************/
/**
 * Created by greguska on 7/18/16.
 */


@Grapes([
        @Grab(group = 'org.nasa.jpl.nexus', module = 'nexus-messages', version = '1.1.0.RELEASE')
])

import org.nasa.jpl.nexus.ingest.wiretypes.NexusContent


includeData = Boolean.parseBoolean(binding.variables.get("includeData"))

if(includeData){
    NexusContent.NexusTile.newBuilder().mergeFrom(payload).toString()
}else {
    NexusContent.NexusTile.newBuilder().mergeFrom(payload).summary.toString()
}