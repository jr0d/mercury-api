# mercury_api
Frontend HTTP API for mercury

# Docker
## Building a local test image

Pre-requisite: Needs a pre-built image of [mercury-core](https://github.com/jr0d/mercury#building-a-local-test-image).

To build the image use the following command

```
docker build -t local/mercury-api -f Dockerfile .
```

## Starting the api

Once the local image has been built, use the compose file to
start the mercury-api service.

```
docker-compose -f docker-compose.yml -p mercury-api up
```
