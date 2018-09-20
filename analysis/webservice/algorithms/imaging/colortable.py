import numpy as np
import math


class ColorTable:

    def __init__(self, identifier, name, spec):
        self.identifier = identifier
        self.name = name
        self.spec = np.array(spec)

    def get_color(self, fraction):
        """
        Determines the color of a value given it's fraction within in a 0-1 range of the min/max and a colortable
        :param fraction: The floating-point value within a range 0 and 1
        :return: The color with values within the range of 0 and 255 as (red, green, blue, alpha)
        """

        index = fraction * (len(self.spec) - 1.0)
        low_index = int(math.floor(index))
        high_index = int(math.ceil(index))

        f = index - low_index
        low_color = self.spec[low_index]
        high_color = self.spec[high_index]

        rgb = high_color * f + low_color * (1.0 - f)
        rgb = np.append(rgb, 255)
        return rgb