variable "aws_region" {
  default = "us-east-1"
}

# variable "lambda_function_name" {
#   default = "Xls2Csv"
# }

variable "key_pair_name" {
  default = "ec2_key_pair"
}

variable "airflow_subnet_id" {
  default = "subnet-0baf7ac9f346c734a"
}

variable "vpc_id" {
  default = "vpc-056b58157ef1dd44b"
}
