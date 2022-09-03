# Creates S3 objects

resource "aws_s3_object" "etl01" {
  bucket = aws_s3_bucket.bucket-emr-files.id
  key    = "etl/01_transform_silver_oil_fuels_sales.py"
  acl    = "private"
  source = "../etl/01_transform_silver_oil_fuels_sales.py"
  etag   = filemd5("../etl/01_transform_silver_oil_fuels_sales.py")
}

resource "aws_s3_object" "etl02" {
  bucket = aws_s3_bucket.bucket-emr-files.id
  key    = "etl/02_transform_silver_diesel_sales.py"
  acl    = "private"
  source = "../etl/02_transform_silver_diesel_sales.py"
  etag   = filemd5("../etl/02_transform_silver_diesel_sales.py")
}

resource "aws_s3_object" "etl03" {
  bucket = aws_s3_bucket.bucket-emr-files.id
  key    = "etl/03_transform_gold_oil_fuels_sales.py"
  acl    = "private"
  source = "../etl/03_transform_gold_oil_fuels_sales.py"
  etag   = filemd5("../etl/03_transform_gold_oil_fuels_sales.py")
}

resource "aws_s3_object" "etl04" {
  bucket = aws_s3_bucket.bucket-emr-files.id
  key    = "etl/04_transform_gold_diesel_sales.py"
  acl    = "private"
  source = "../etl/04_transform_gold_diesel_sales.py"
  etag   = filemd5("../etl/04_transform_gold_diesel_sales.py")
}
