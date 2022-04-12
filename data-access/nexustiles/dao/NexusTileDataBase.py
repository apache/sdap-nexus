import numpy as np

class NexusTileDataBase:
    def get_raw_data_array(self):

        nexus_tile = self._get_nexus_tile()
        the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

        the_tile_data = getattr(nexus_tile.tile, the_tile_type)

        return from_shaped_array(the_tile_data.variable_data)

    def get_lat_lon_time_data_meta(self):
        """
        Retrieve data from data store and metadata from metadata store
        for this tile. For gridded tiles, the tile shape of the data
        will match the input shape. For example, if the input was a
        30x30 tile, all variables will also be 30x30. However, if the
        tile is a swath tile, the data will be transformed along the
        diagonal of the data matrix. For example, a 30x30 tile would
        become 900x900 where the 900 points are along the diagonal.

        Multi-variable tile will also include an extra dimension in the
        data array. For example, a 30 x 30 x 30 array would be
        transformed to N x 30 x 30 x 30 where N is the number of
         variables in this tile.

        latitude_data, longitude_data, np.array([grid_tile.time]), grid_tile_data, meta_data, is_multi_var

        :return: latitude data
        :return: longitude data
        :return: time data
        :return: data
        :return: meta data dictionary
        :return: boolean flag, True if this tile has more than one variable
        """
        is_multi_var = False

        if self._get_nexus_tile().HasField('grid_tile'):
            grid_tile = self._get_nexus_tile().grid_tile

            grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_tile.variable_data))
            latitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(grid_tile.longitude))

            if len(grid_tile_data.shape) == 2:
                grid_tile_data = grid_tile_data[np.newaxis, :]

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in grid_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                if len(meta_array.shape) == 2:
                    meta_array = meta_array[np.newaxis, :]
                meta_data[name] = meta_array

            return latitude_data, longitude_data, np.array([grid_tile.time]), grid_tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('swath_tile'):
            swath_tile = self._get_nexus_tile().swath_tile

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude)).reshape(-1)
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude)).reshape(-1)
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time)).reshape(-1)

            # Simplify the tile if the time dimension is the same value repeated
            if np.all(time_data == np.min(time_data)):
                time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))

            tile_data = self._to_standard_index(swath_tile_data,
                                                (len(time_data), len(latitude_data), len(longitude_data)))

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                reshaped_meta_array = self._to_standard_index(actual_meta_array, tile_data.shape)
                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('time_series_tile'):
            time_series_tile = self._get_nexus_tile().time_series_tile

            time_series_tile_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.variable_data))
            time_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.time)).reshape(-1)
            latitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(time_series_tile.longitude))

            reshaped_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
            idx = np.arange(len(latitude_data))
            reshaped_array[:, idx, idx] = time_series_tile_data
            tile_data = reshaped_array
            # Extract the meta data
            meta_data = {}
            for meta_data_obj in time_series_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))

                reshaped_meta_array = np.ma.masked_all((len(time_data), len(latitude_data), len(longitude_data)))
                idx = np.arange(len(latitude_data))
                reshaped_meta_array[:, idx, idx] = meta_array

                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('swath_multi_variable_tile'):
            swath_tile = self._get_nexus_tile().swath_multi_variable_tile
            is_multi_var = True

            latitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.latitude)).reshape(-1)
            longitude_data = np.ma.masked_invalid(from_shaped_array(swath_tile.longitude)).reshape(-1)
            time_data = np.ma.masked_invalid(from_shaped_array(swath_tile.time)).reshape(-1)

            # Simplify the tile if the time dimension is the same value repeated
            if np.all(time_data == np.min(time_data)):
                time_data = np.array([np.min(time_data)])

            swath_tile_data = np.ma.masked_invalid(from_shaped_array(swath_tile.variable_data))

            desired_shape = (
                len(time_data),
                len(latitude_data),
                len(longitude_data),
            )
            tile_data = self._to_standard_index(swath_tile_data, desired_shape, is_multi_var=True)

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in swath_tile.meta_data:
                name = meta_data_obj.name
                actual_meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                reshaped_meta_array = self._to_standard_index(actual_meta_array, tile_data.shape)
                meta_data[name] = reshaped_meta_array

            return latitude_data, longitude_data, time_data, tile_data, meta_data, is_multi_var
        elif self._get_nexus_tile().HasField('grid_multi_variable_tile'):
            grid_multi_variable_tile = self._get_nexus_tile().grid_multi_variable_tile
            is_multi_var = True

            grid_tile_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.variable_data))
            latitude_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.latitude))
            longitude_data = np.ma.masked_invalid(from_shaped_array(grid_multi_variable_tile.longitude))

            # If there are 3 dimensions, that means the time dimension
            # was squeezed. Add back in
            if len(grid_tile_data.shape) == 3:
                grid_tile_data = np.expand_dims(grid_tile_data, axis=1)
            # If there are 4 dimensions, that means the time dimension
            # is present. Move the multivar dimension.
            if len(grid_tile_data.shape) == 4:
                grid_tile_data = np.moveaxis(grid_tile_data, -1, 0)

            # Extract the meta data
            meta_data = {}
            for meta_data_obj in grid_multi_variable_tile.meta_data:
                name = meta_data_obj.name
                meta_array = np.ma.masked_invalid(from_shaped_array(meta_data_obj.meta_data))
                if len(meta_array.shape) == 2:
                    meta_array = meta_array[np.newaxis, :]
                meta_data[name] = meta_array

            return latitude_data, longitude_data, np.array([grid_multi_variable_tile.time]), grid_tile_data, meta_data, is_multi_var
        else:
            raise NotImplementedError("Only supports grid_tile, swath_tile, swath_multi_variable_tile, and time_series_tile")

    @staticmethod
    def _to_standard_index(data_array, desired_shape, is_multi_var=False):
        """
        Transform swath data to a standard format where data runs along
        diagonal of ND matrix and the non-diagonal data points are
        masked

        :param data_array: The data array to be transformed
        :param desired_shape: The desired shape of the resulting array
        :param is_multi_var: True if this is a multi-variable tile
        :type data_array: np.array
        :type desired_shape: tuple
        :type is_multi_var: bool
        :return: Reshaped array
        :rtype: np.array
        """

        if desired_shape[0] == 1:
            reshaped_array = np.ma.masked_all((desired_shape[1], desired_shape[2]))
            row, col = np.indices(data_array.shape)

            reshaped_array[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array[
                row.flat, col.flat]
            reshaped_array.mask[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array.mask[
                row.flat, col.flat]
            reshaped_array = reshaped_array[np.newaxis, :]
        elif is_multi_var == True:
            # Break the array up by variable. Translate shape from
            # len(times) x len(latitudes) x len(longitudes) x num_vars,
            # to
            # num_vars x len(times) x len(latitudes) x len(longitudes)
            reshaped_data_array = np.moveaxis(data_array, -1, 0)
            reshaped_array = []

            for variable_data_array in reshaped_data_array:
                variable_reshaped_array = np.ma.masked_all(desired_shape)
                row, col = np.indices(variable_data_array.shape)

                variable_reshaped_array[np.diag_indices(desired_shape[1], len(variable_reshaped_array.shape))] = variable_data_array[
                    row.flat, col.flat]
                variable_reshaped_array.mask[np.diag_indices(desired_shape[1], len(variable_reshaped_array.shape))] = variable_data_array.mask[
                    row.flat, col.flat]
                reshaped_array.append(variable_reshaped_array)
        else:
            reshaped_array = np.ma.masked_all(desired_shape)
            row, col = np.indices(data_array.shape)

            reshaped_array[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array[
                row.flat, col.flat]
            reshaped_array.mask[np.diag_indices(desired_shape[1], len(reshaped_array.shape))] = data_array.mask[
                row.flat, col.flat]

        return reshaped_array
