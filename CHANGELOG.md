# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- SDAP-388: Enable SDAP to proxy/redirect to alternate SDAP
- SDAP-372: Updated matchup algorithm to point to AWS insitu API endpoint
- SDAP-372: Added new matchup endpoint `match_spark_doms` that points to DOMS insitu endpoint
- SDAP-372: Updated `match_spark_doms` to interface with samos_cdms endpoint 
- SDAP-393: Included `insitu` in ingress based on the value of `insituAPI.enabled` in `values.yaml`
- SDAP-371: Renamed `/domssubset` endpoint to `/cdmssubset`
- SDAP-390: Updated NetCDF reader tool for data matchup and added user functionality.
- SDAP-396: Added saildrone insitu api to matchup
- SDAP-398: Added script for regression tests.
### Changed
- SDAP-390: Changed `/doms` to `/cdms` and `doms_reader.py` to `cdms_reader.py`
- domslist endpoint points to AWS insitu instead of doms insitu
### Deprecated
### Removed
- removed dropdown from matchup doms endpoint secondary param
### Fixed
- Fix failing test_matchup unit test
- Fixed bug in OpenAPI spec where both matchup endpoints shared the same id
- Fixed null value for count in matchup response
- SDAP-371: Fixed DOMS subset endpoint
- SDAP-397: Added depth to matchup insitu output
- SDAP-397: Pull ID from insitu api response for matchup
- SDAP-397: Added null unit field for later use in matchup
- Fixed issue where satellite satellite matchups failed
- Fixed issue where null results were returned if more than "resultSizeLimit" matches are found
- Preserve return value of `query_insitu_schema()` in matchup to avoid excessive API hits 
- Fixed issue where satellite to satellite matchups with the same dataset don't return the expected result
- Fixed CSV and NetCDF matchup output bug
- Fixed NetCDF output switching latitude and longitude
- Fixed import error causing `/timeSeriesSpark` queries to fail.
### Security
