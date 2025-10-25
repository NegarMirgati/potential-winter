from typing import Optional
import io
import os
from minio import Minio
from minio.error import S3Error


def connect_minio(
    endpoint: str, access_key: str, secret_key: str, secure: bool = True
) -> Minio:
    """
    Create and return a Minio client.
    """
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def ensure_bucket(
    client: Minio, bucket_name: str, location: Optional[str] = None
) -> None:
    """
    Create bucket if it doesn't exist.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name, location=location)
    except S3Error as err:
        raise RuntimeError(f"Failed to ensure bucket '{bucket_name}': {err}")


def store_data(
    client: Minio,
    bucket_name: str,
    object_name: str,
    *,
    data: Optional[bytes] = None,
    file_path: Optional[str] = None,
    content_type: str = "application/octet-stream",
) -> None:
    """
    Store data in an object. Provide either `data` (bytes) or `file_path` (str).
    This will ensure the bucket exists before upload.
    """
    if (data is None) == (file_path is None):
        raise ValueError("Provide exactly one of `data` or `file_path`.")

    ensure_bucket(client, bucket_name)

    try:
        if data is not None:
            bio = io.BytesIO(data)
            bio.seek(0)
            client.put_object(
                bucket_name,
                object_name,
                bio,
                length=len(data),
                content_type=content_type,
            )
        else:
            file_size = os.path.getsize(file_path)
            with open(file_path, "rb") as fp:
                client.put_object(
                    bucket_name,
                    object_name,
                    fp,
                    length=file_size,
                    content_type=content_type,
                )
    except S3Error as err:
        raise RuntimeError(
            f"Failed to store object '{object_name}' in bucket '{bucket_name}': {err}"
        )


# Example usage (fill in real values)
def get_minio_client(
    endpoint: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    secure: Optional[bool] = None,
) -> Minio:
    """
    Return a configured Minio client using username/password authentication.

    Environment variables checked:
      - MINIO_ENDPOINT
      - MINIO_USERNAME
      - MINIO_PASSWORD
      - MINIO_SECURE
    """
    endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    username = username or os.environ.get("MINIO_USERNAME") or "superuser"
    password = password or os.environ.get("MINIO_PASSWORD") or "superuser"

    if secure is None:
        secure_env = os.environ.get("MINIO_SECURE", "false")
        secure = secure_env.lower() in ("1", "true", "yes")

    return connect_minio(
        endpoint, access_key=username, secret_key=password, secure=secure
    )


if __name__ == "__main__":
    # Run directly for a quick smoke test
    client = get_minio_client()
    ensure_bucket(client, "my-bucket", location="us-east-1")
    store_data(client, "my-bucket", "hello.txt", data=b"hello from minio")
