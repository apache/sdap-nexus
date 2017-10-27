PLATFORMS = [
    {"id": 1, "desc": "ship"},
    {"id": 2, "desc": "moored surface buoy"},
    {"id": 3, "desc": "drifting surface float"},
    {"id": 4, "desc": "drifting subsurface profiling float"},
    {"id": 5, "desc": "autonomous underwater vehicle"},
    {"id": 6, "desc": "offshore structure"},
    {"id": 7, "desc": "coastal structure"},
    {"id": 8, "desc": "towed unmanned submersible"},
    {"id": 9, "desc": "orbiting satellite"}
]

DEVICES = [
    {"id": 1, "desc": "bathythermographs"},
    {"id": 2, "desc": "discrete water samplers"},
    {"id": 3, "desc": "CTD"},
    {"id": 4, "desc": "Current profilers  / acousticDopplerCurrentProfiler"},
    {"id": 5, "desc": "radiometers"},
    {"id": 6, "desc": "scatterometers"}
]

MISSIONS = [
    {"id": 1, "desc": "SAMOS"},
    {"id": 2, "desc": "ICOADS"},
    {"id": 3, "desc": "Aquarius"},
    {"id": 4, "desc": "SPURS1"}
]


def getDescById(list, id):
    for item in list:
        if item["id"] == id:
            return item["desc"]
    return id


def getPlatformById(id):
    return getDescById(PLATFORMS, id)


def getDeviceById(id):
    return getDescById(DEVICES, id)


def getMissionById(id):
    return getDescById(MISSIONS, id)


def getDescByListNameAndId(listName, id):
    if listName.upper() == "PLATFORM":
        return getPlatformById(id)
    elif listName.upper() == "DEVICE":
        return getDeviceById(id)
    elif listName.upper() == "MISSION":
        return getMissionById(id)
    else:
        raise Exception("Invalid list name specified ('%s')" % listName)
