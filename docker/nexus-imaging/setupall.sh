cd /tmp/image-gen-nexus/nexus-ingest/nexus-messages
./gradlew clean build install
cd /build/python/nexusproto

conda create --name nexus python
source activate nexus

conda install numpy
python setup.py install

cd /tmp/image-gen-nexus/data-access
pip install cython
python setup.py install

cd /tmp/image-gen-nexus/analysis
conda install numpy matplotlib mpld3 scipy netCDF4 basemap gdal pyproj=1.9.5.1 libnetcdf=4.3.3.1
pip install pillow
python setup.py install