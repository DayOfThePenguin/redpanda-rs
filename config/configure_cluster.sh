# declarative configuration to set up the s3 archiving of data from Redpanda to MinIO

# MinIO Username
docker exec panda-0 rpk cluster config set cloud_storage_access_key redpanda

# MinIO Password
docker exec panda-0 rpk cluster config set cloud_storage_secret_key panda12345

# MinIO Domain
docker exec panda-0 rpk cluster config set cloud_storage_api_endpoint minio-s3

# MinIO Port
docker exec panda-0 rpk cluster config set cloud_storage_api_endpoint_port 9000

# MinIO Bucket
docker exec panda-0 rpk cluster config set cloud_storage_bucket panda-bucket

# Turn off TLS because this is a simple demo
docker exec panda-0 rpk cluster config set cloud_storage_disable_tls true

# MinIO Region
docker exec panda-0 rpk cluster config set cloud_storage_region panda-region

# Enable remote write (from S3)
docker exec panda-0 rpk cluster config set cloud_storage_enable_remote_write true

# Enable remote read (from S3)
docker exec panda-0 rpk cluster config set cloud_storage_enable_remote_read true

# Turn on archiving to cloud storage
docker exec panda-0 rpk cluster config set cloud_storage_enabled true

# Print the status of the Redpanda cluster (a restart of your cluster will probably be
# required after you run this for the first time)
docker exec panda-0 rpk cluster config status