## SDAP regression tests

### To run:

```shell
pytest test_cdms.py --with-integration [--force-subset]
```

### Options

- `--force-subset`: Force the test for the `/cdmssubset` endpoint to be run. It is currently skipped by default until Zarr functionality
can be incorporated into the subsetter algorithm.


### Environment variables:

| Name                    | Function                                                                                                                                               |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| TEST_HOST               | Host for SDAP. Ex `http://localhost:8083/`. Default: `http://doms.jpl.nasa.gov`                                                                        |
| INSITU_ENDPOINT         | Endpoint URL for in-situ API calls. Default: `http://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination`                                   |
| INSITU_SWAGGER_ENDPOINT | Endpoint URL for in-situ SwaggerUI. Default: `http://doms.jpl.nasa.gov/insitu/1.0/insitu_query_swagger/`                                               |

NOTE: When the CDMS Reader tool is available, replace `cdms_reader.py` in this directory with a soft link to `/tools/cdms/cdms_reader.py`