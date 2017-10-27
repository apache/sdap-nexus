analysis
=====

Python module that exposes NEXUS analytical capabilities via a HTTP webservice. Accessible endpoints are described on the [API](https://github.com/dataplumber/nexus/wiki/API) page of the wiki.

# Developer Setup

**NOTE** This project has a dependency on [data-access](https://github.jpl.nasa.gov/thuang/nexus/tree/master/data-access). Make sure data-access is installed in the same environment you will be using for this module.

1. Setup a separate conda env or activate an existing one

    ````
    conda create --name nexus-analysis python
    source activate nexus-analysis
    ````

2. Install conda dependencies

    ````
    conda install numpy matplotlib mpld3 scipy netCDF4 basemap gdal pyproj=1.9.5.1 libnetcdf=4.3.3.1
    ````

3. Run `python setup.py install`

4. Launch `python webservice/webapp.py`
