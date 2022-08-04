# Creates S3 objects

resource "aws_s3_object" "delta_insert" {
  bucket = aws_s3_bucket.bucket-emr-files.id
  key    = "pyspark/01_delta_spark_insert.py"
  acl    = "private"
  source = "../etl/01_delta_spark_insert.py"
  etag   = filemd5("../etl/01_delta_spark_insert.py")
}

resource "aws_s3_object" "delta_upsert" {
  bucket = aws_s3_bucket.bucket-emr-files.id
  key    = "pyspark/02_delta_spark_upsert.py"
  acl    = "private"
  source = "../etl/02_delta_spark_upsert.py"
  etag   = filemd5("../etl/02_delta_spark_upsert.py")
}
