/*****************************************************************************
 * Copyright (c) 2016 Jet Propulsion Laboratory,
 * California Institute of Technology.  All rights reserved
 *****************************************************************************/
/**
 * Created by Nga Quach on 8/31/16.
 */

if (timelen == null) {
    throw new RuntimeException("This script requires the length of the time array.")
}
def time = 'time'
if (binding.variables.get("timevar") != null) {
    time = timevar
}

def specsIn = payload
if (payload instanceof String) {
    specsIn = [payload]
}

def length = timelen.toInteger()
def specsOut = []
for (i = 0; i < length; i++) {
    specsOut.addAll(specsIn.collect{ spec ->
        "$time:$i:${i+1},$spec"
    })
}

def sectionSpec = specsOut.join(';')
sectionSpec <<= ';file://'
sectionSpec <<= headers.absolutefilepath

return sectionSpec.toString()
