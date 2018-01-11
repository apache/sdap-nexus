import math

MEAN_RADIUS_EARTH_METERS = 6371010.0
EQUATORIAL_RADIUS_EARTH_METERS = 6378140.0
POLAR_RADIUS_EARTH_METERS = 6356752.0
FLATTENING_EARTH = 298.257223563
MEAN_RADIUS_EARTH_MILES = 3958.8


class DistanceUnit(object):
    METERS = 0
    MILES = 1


# Haversine implementation for great-circle distances between two points
def haversine(x0, y0, x1, y1, units=DistanceUnit.METERS):
    if units == DistanceUnit.METERS:
        R = MEAN_RADIUS_EARTH_METERS
    elif units == DistanceUnit.MILES:
        R = MEAN_RADIUS_EARTH_MILES
    else:
        raise Exception("Invalid units specified")
    x0r = x0 * (math.pi / 180.0)  # To radians
    x1r = x1 * (math.pi / 180.0)  # To radians
    xd = (x1 - x0) * (math.pi / 180.0)
    yd = (y1 - y0) * (math.pi / 180.0)

    a = math.sin(xd / 2.0) * math.sin(xd / 2.0) + \
        math.cos(x0r) * math.cos(x1r) * \
        math.sin(yd / 2.0) * math.sin(yd / 2.0)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    d = R * c
    return d


# Equirectangular approximation for when performance is key. Better at smaller distances
def equirectangularApprox(x0, y0, x1, y1):
    R = 6371000.0  # Meters
    x0r = x0 * (math.pi / 180.0)  # To radians
    x1r = x1 * (math.pi / 180.0)
    y0r = y0 * (math.pi / 180.0)
    y1r = y1 * (math.pi / 180.0)

    x = (y1r - y0r) * math.cos((x0r + x1r) / 2.0)
    y = x1r - x0r
    d = math.sqrt(x * x + y * y) * R
    return d


class BoundingBox(object):

    def __init__(self, north=None, south=None, west=None, east=None, asString=None):
        if asString is not None:
            bboxParts = asString.split(",")
            self.west = float(bboxParts[0])
            self.south = float(bboxParts[1])
            self.east = float(bboxParts[2])
            self.north = float(bboxParts[3])
        else:
            self.north = north
            self.south = south
            self.west = west
            self.east = east

    def toString(self):
        return "%s,%s,%s,%s" % (self.west, self.south, self.east, self.north)

    def toMap(self):
        return {
            "xmin": self.west,
            "xmax": self.east,
            "ymin": self.south,
            "ymax": self.north
        }


'''
    Constrains, does not expand.
'''


class BoundsConstrainer(BoundingBox):

    def __init__(self, north=None, south=None, west=None, east=None, asString=None):
        BoundingBox.__init__(self, north, south, west, east, asString)

    def testNorth(self, v):
        if v is None:
            return
        self.north = max([self.north, v])

    def testSouth(self, v):
        if v is None:
            return
        self.south = min([self.south, v])

    def testEast(self, v):
        if v is None:
            return
        self.east = max([self.east, v])

    def testWest(self, v):
        if v is None:
            return
        self.west = min([self.west, v])

    def testCoords(self, north=None, south=None, west=None, east=None):
        self.testNorth(north)
        self.testSouth(south)
        self.testWest(west)
        self.testEast(east)

    def testOtherConstrainer(self, other):
        self.testCoords(north=other.north, south=other.south, west=other.west, east=other.east)
