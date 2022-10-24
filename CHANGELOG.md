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
- Matchup validates insitu parameter using insitu API schema endpoint
- Added domsresults endpoint to openapi spec
- Added markdown table to matchup `platform` param in openapi spec
- SDAP-400: Added NCAR insitu api to matchup
- SDAP-405: Added SPURS AWS insitu api to matchup and new platform values to OpenAPI matchup spec
- RabbitMQ monitor script used in Docker quickstart guide
- Added new option for NCAR so either NCAR or JPL Insitu API can be specified
### Changed
- SDAP-390: Changed `/doms` to `/cdms` and `doms_reader.py` to `cdms_reader.py`
- domslist endpoint points to AWS insitu instead of doms insitu
- Matchup returns numSecondary and numPrimary counts rather than insitu/gridded
- SDAP-402: Changed matchup matchOnce logic to match multiple points if same time/space
- Bumped ingress timeout in Helm chart to reflect AWS gateway timeout
- SDAP-399: Updated quickstart guide for standalone docker deployment of SDAP.
- SDAP-399: Updated quickstart Jupyter notebook
- Added logging message for start of insitu query + added status code & elapsed time to post query log message.
- Added explicit timeouts for all insitu related queries to prevent hanging issue.
### Deprecated
### Removed
- removed dropdown from matchup doms endpoint secondary param
- Matchup no longer returns insituMatched and griddedMatched fields
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
- Fixed bug where domsresults no longer worked after successful matchup
- Fixed certificate error in Dockerfile
- SDAP-403: Remote timeout fix and HofMoeller bug fix
- Fixed matchup insitu query loading on import; loads when needed instead
- SDAP-406: Fixed `/timeSeriesSpark`comparison stats bug
### Security


