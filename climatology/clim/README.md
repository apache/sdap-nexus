# climatology-gaussianInterp module

Compute N-day climatology from daily ocean data files.

See bottom of climatology2.py for examples of what to run.

For speedup, the gaussian weighting is also implemented in Fortran
and cython.  The pure python version is abysmally slow, of course,
and even the Fortran takes a while (evaluating too many exponentials).

To build gaussInter_f.so from gaussInterp_f.f, run the line in
gaussInterp_f.mk.



3 Climatology’s to generate. Daily data is already downloaded to server-1. Location is (a) shown below
 
1.      MODIS CHL_A
a.      server-1:/data/share/datasets/MODIS_L3m_DAY_CHL_chlor_a_4km/daily_data
b.      https://oceandata.sci.gsfc.nasa.gov/MODIS-Aqua/Mapped/Daily/4km/chlor_a

2.      Measures SSH
a.      server-1:/data/share/datasets/MEASURES_SLA_JPL_1603/daily_data
b.      http://podaac.jpl.nasa.gov/dataset/SEA_SURFACE_HEIGHT_ALT_GRIDS_L4_2SATS_5DAY_6THDEG_V_JPL1609
c.       ftp://podaac-ftp.jpl.nasa.gov/allData/merged_alt/L4/cdr_grid/

3.      CCMP Wind
a.      server-1:/data/share/datasets/CCMP_V2.0_L3.0/daily_data/
b.      https://podaac.jpl.nasa.gov/dataset/CCMP_MEASURES_ATLAS_L4_OW_L3_0_WIND_VECTORS_FLK
 


