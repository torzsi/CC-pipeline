import gzip
from io import BytesIO

from cc_pipeline.warc import S3WarcReader, split_s3_uri


def test_split_s3_uri_supports_explicit_and_implicit_commoncrawl_paths() -> None:
    assert split_s3_uri("s3://commoncrawl/crawl-data/a.warc.gz") == ("commoncrawl", "crawl-data/a.warc.gz")
    assert split_s3_uri("crawl-data/a.warc.gz") == ("commoncrawl", "crawl-data/a.warc.gz")


def test_s3_warc_reader_reads_gzip_record() -> None:
    payload = (
        b"WARC/1.0\r\n"
        b"WARC-Type: response\r\n"
        b"WARC-Target-URI: https://example.com/post\r\n"
        b"Content-Length: 140\r\n"
        b"\r\n"
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><p>hello from s3</p></body></html>\r\n"
    )
    compressed = gzip.compress(payload)

    class FakeBody:
        def read(self):
            return compressed

    class FakeS3Client:
        def get_object(self, **kwargs):
            assert kwargs["Bucket"] == "commoncrawl"
            assert kwargs["Key"] == "crawl-data/a.warc.gz"
            assert kwargs["Range"] == "bytes=0-11"
            return {"Body": FakeBody()}

    class FakeSession:
        def client(self, service_name):
            assert service_name == "s3"
            return FakeS3Client()

    reader = S3WarcReader(boto3_session=FakeSession())
    record = reader.read_record("crawl-data/a.warc.gz", offset=0, length=12)

    assert record.target_uri == "https://example.com/post"
    assert record.content_type == "text/html"
    assert "hello from s3" in record.text()
