import time
from datetime import datetime

__LAYERS__ = []


def colorbar(productLabel,
             minValue,
             maxValue,
             colorbarName,
             units
             ):
    return {
        "productLabel": productLabel,
        "minValue": minValue,
        "maxValue": maxValue,
        "colorbarName": colorbarName,
        "units": units
    }

def bounding(north=90.0,
           south=-90.0,
           east=180.0,
           west=-180.0):
    return {
        "north": north,
        "south": south,
        "east": east,
        "west": west
    }

def depthspec(title="Depth", units="m", labels=[], values=[]):

    return {
        "title": title,
        "labels": labels,
        "values": values,
        "units": units,
    }

def addlayer(
    productLabel,
    crmShortName=None,
    enabled=True,
    mission="",
    instrument="Unspecified",
    parameter="Unspecified",
    productType="Mosaic",
    serviceProtocol="GIBS",
    endpoint="",
    layerTitle="",
    layerService="wmtstiled",
    hasGlobal=True,
    hasNorth=True,
    hasSouth=True,
    thumbnailImage=None,
    layerSubtitle="",
    availableForAnalysis=False,
    analysisDatasetName="",
    layerProjectionGlobal="EPSG:4326",
    layerProjectionNorth="EPSG:3413",
    layerProjectionSouth="EPSG:3031",
    wmtsGlobalMatrixSet="EPSG4326_500m",
    wmtsNorthMatrixSet="EPSG3413_250m",
    wmtsSouthMatrixSet="EPSG3031_250m",
    wmtsLayerName="",
    wmtsFormat="image/png",
    colorbar="",
    availability=None,
    keywords=[],
    envs=["DEV", "SIT", "UAT", "PROD"],
    utilityLayer=False,
    baseLayer=False,
    colorbarSpec=None,
    depthSpec=None,
    bounding=bounding(),
    isGrayscale=False,
    type="observation"):

    global __LAYERS__

    layer = {
        "productLabel": productLabel,
        "crmShortName": crmShortName,
        "enabled": enabled,
        "mission": mission,
        "instrument": instrument,
        "parameter": parameter,
        "productType": productType,
        "serviceProtocol": serviceProtocol,
        "endpoint": endpoint,
        "layerTitle": layerTitle,
        "layerService": layerService,
        "hasGlobal": hasGlobal,
        "hasNorth": hasNorth,
        "hasSouth": hasSouth,
        "thumbnailImage": thumbnailImage,
        "bounding": bounding,
        "layerSubtitle": layerSubtitle,
        "availableForAnalysis": availableForAnalysis,
        "analysisDatasetName": analysisDatasetName,
        "layerProjectionGlobal": layerProjectionGlobal,
        "layerProjectionNorth": layerProjectionNorth,
        "layerProjectionSouth": layerProjectionSouth,
        "wmtsGlobalMatrixSet": wmtsGlobalMatrixSet,
        "wmtsNorthMatrixSet": wmtsNorthMatrixSet,
        "wmtsSouthMatrixSet": wmtsSouthMatrixSet,
        "wmtsLayerName": wmtsLayerName,
        "wmtsFormat": wmtsFormat,
        "colorbar": colorbar,
        "availability": availability,
        "keywords": keywords,
        "envs": envs,
        "utilityLayer": utilityLayer,
        "baseLayer": baseLayer,
        "colorbarSpec": colorbarSpec,
        "depthSpec": depthSpec,
        "isGrayscale": isGrayscale,
        "type":type
    }

    __LAYERS__.append(layer)



def timestamp(year, month, day):
    dt = datetime(year, month, day)
    ts = time.mktime(dt.timetuple())
    return int(ts * 1000)


def getLayers():
    return __LAYERS__