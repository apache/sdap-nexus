class StatsComputeOptions(object):
    def __init__(self):
        pass

    def get_apply_seasonal_cycle_filter(self, default="false"):
        raise Exception("Please implement")

    def get_max_lat(self, default=90.0):
        raise Exception("Please implement")

    def get_min_lat(self, default=-90.0):
        raise Exception("Please implement")

    def get_max_lon(self, default=180):
        raise Exception("Please implement")

    def get_min_lon(self, default=-180):
        raise Exception("Please implement")

    def get_dataset(self):
        raise Exception("Please implement")

    def get_environment(self):
        raise Exception("Please implement")

    def get_start_time(self):
        raise Exception("Please implement")

    def get_end_time(self):
        raise Exception("Please implement")

    def get_start_year(self):
        raise Exception("Please implement")

    def get_end_year(self):
        raise Exception("Please implement")

    def get_clim_month(self):
        raise Exception("Please implement")

    def get_start_row(self):
        raise Exception("Please implement")

    def get_end_row(self):
        raise Exception("Please implement")

    def get_content_type(self):
        raise Exception("Please implement")

    def get_apply_low_pass_filter(self, default=False):
        raise Exception("Please implement")

    def get_low_pass_low_cut(self, default=12):
        raise Exception("Please implement")

    def get_low_pass_order(self, default=9):
        raise Exception("Please implement")

    def get_plot_series(self, default="mean"):
        raise Exception("Please implement")

    def get_plot_type(self, default="default"):
        raise Exception("Please implement")

    def get_nparts(self):
        raise Exception("Please implement")