# Summarize Yesterday

```shell
docker build -t hub.opensciencegrid.org/opensciencegrid/summarize-yesterday-adstash:latest --file images/summarize_yesterday/Dockerfile .
```

```shell
docker run --env-file .env hub.opensciencegrid.org/opensciencegrid/summarize-yesterday-adstash:latest
```

```shell
docker push hub.opensciencegrid.org/opensciencegrid/summarize-yesterday-adstash:latest
```
