# Creates S3 buckets

resource "aws_s3_bucket" "bucket-bronze" {
  bucket = "anp-bronze"

  tags = {
    IAC     = "TF",
    PROJECT = "ANP FUEL SALES"
  }
}

resource "aws_s3_bucket_acl" "bucket-bronze-acl" {
  bucket = aws_s3_bucket.bucket-bronze.id
  acl    = "private"
}

resource "aws_s3_bucket" "bucket-silver" {
  bucket = "anp-silver"

  tags = {
    IAC     = "TF",
    PROJECT = "ANP FUEL SALES"
  }
}

resource "aws_s3_bucket_acl" "bucket-silver-acl" {
  bucket = aws_s3_bucket.bucket-silver.id
  acl    = "private"
}

resource "aws_s3_bucket" "bucket-gold" {
  bucket = "anp-gold"

  tags = {
    IAC     = "TF",
    PROJECT = "ANP FUEL SALES"
  }
}

resource "aws_s3_bucket_acl" "bucket-gold-acl" {
  bucket = aws_s3_bucket.bucket-gold.id
  acl    = "private"
}

resource "aws_s3_bucket" "bucket-etl-files" {
  bucket = "anp-etl-files"

  tags = {
    IAC     = "TF",
    PROJECT = "ANP FUEL SALES"
  }
}

resource "aws_s3_bucket_acl" "bucket-etl-files-acl" {
  bucket = aws_s3_bucket.bucket-etl-files.id
  acl    = "private"
}
