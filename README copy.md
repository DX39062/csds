# SDCS

```
docker load -i ubuntu2004.tar

docker compose down
docker compose up -build

docker build -t sdcs-test .
docker run sdcs-test