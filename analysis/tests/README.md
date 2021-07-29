# Apache SDAP Testing

## Unit Tests

Unit tests don't contain any externally running dependencies (like Solr 
or Cassandra). Unit tests, unlike integration tests, do not contain an 
'integration' pytest marker. To run all unit tests and skip integration 
tests, run the following command:

```shell script
pytest -m "not integration" analysis/tests/
```

## Integration Tests

To run integration tests, Cassandra and Solr must be running locally.

Integration tests have been marked with 'integration' pytest markers. 
In order to run only integration tests, run the following command:

```shell script
pytest -m "integration" analysis/tests/
```
