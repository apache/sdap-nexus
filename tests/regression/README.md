## SDAP regression tests

To run:

```shell
pytest test_cdms.py
```

Environment variables:

| Name                    | Function                                                                                                                                               |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| TEST_HOST               | Host for SDAP. Ex `http://localhost:8083/`. Default: `http://doms.jpl.nasa.gov`                                                                        |
| INSITU_ENDPOINT         | Endpoint URL for in-situ API calls. Default: `http://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination`                                   |
| INSITU_SWAGGER_ENDPOINT | Endpoint URL for in-situ SwaggerUI. Default: `http://doms.jpl.nasa.gov/insitu/1.0/insitu_query_swagger/`                                               |
| SKIP_MATCHUP            | Do not run the `matchup` endpoint tests. Note that this will implicitly also skip `domsresults` tests as they require `matchup` tests to run and pass. |
| SKIP_RESULTS            | Do not run ANY of the `domsresults` endpoint tests.                                                                                                    |
| SKIP_RESULTS_JSON       | Do not run the `domsresults` endpoint tests with JSON output formatting.                                                                               |
| SKIP_RESULTS_CSV        | Do not run the `domsresults` endpoint tests with CSV output formatting.                                                                                |
| SKIP_RESULTS_NETCDF     | Do not run the `domsresults` endpoint tests with NetCDF output formatting.                                                                             |
| SKIP_LIST               | Do not run the `domslist` endpoint test.                                                                                                               |
| SKIP_SUBSET             | Do not run the `cdmssubset` endpoint tests.                                                                                                            |
| SKIP_INSITU             | Do not run the in-situ API test.                                                                                                                       |
| SKIP_SWAGGER_SDAP       | Do not run the SDAP SwaggerUI test.                                                                                                                    |
| SKIP_SWAGGER_INSITU     | Do not run the in-situ SwaggerUI test.                                                                                                                 |

NOTE: When the CDMS Reader tool is available, replace `cdms_reader.py` in this directory with a soft link to `/tools/cdms/cdms_reader.py`