# Creates S3 buckets

resource "aws_s3_bucket" "bucket-bronze" {
  bucket = "anp-bronze"

  tags = {
    IAC     = "TF",
    PROJECT = "ANP FUEL SALES"
  }
}

resource "aws_s3_bucket_acl" "bucket-acl-bronze" {
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

resource "aws_s3_bucket_acl" "bucket-acl-gold" {
  bucket = aws_s3_bucket.bucket-gold.id
  acl    = "private"
}

resource "aws_s3_bucket" "bucket-gold" {
  bucket = "anp-gold"

  tags = {
    IAC     = "TF",
    PROJECT = "ANP FUEL SALES"
  }
}

resource "aws_s3_bucket_acl" "bucket-acl-gold" {
  bucket = aws_s3_bucket.bucket-gold.id
  acl    = "private"
}