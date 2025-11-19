# Summarize Yesterday

```shell
docker build --platform linux/amd64 -t hub.opensciencegrid.org/opensciencegrid/summarize-adstash:latest --file images/summarize_yesterday/Dockerfile .
```

```shell
docker run --env-file .env hub.opensciencegrid.org/opensciencegrid/summarize-adstash:latest
```

```shell
docker push hub.opensciencegrid.org/opensciencegrid/summarize-adstash:latest
```
