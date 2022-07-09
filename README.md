# How to test

Start minio server for testing.

```
docker run -p 9000:9000 --name minio -e "MINIO_ACCESS_KEY=pathlibfs" -e "MINIO_SECRET_KEY=pathlibfs" minio/minio server /data
```

Run test:
```
pytest
```