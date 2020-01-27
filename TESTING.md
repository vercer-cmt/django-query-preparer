# Testing

Django query preparer has unit and integration tests in `dqp/tests/`.  There is test application bundled in this repo (`test_app`) which provides the scaffolding needed to run the tests.

To run the unit tests you need to have docker and docker-compose installed. Then simply do:

```
docker-compose up --build dqp
```

in the base directory.
