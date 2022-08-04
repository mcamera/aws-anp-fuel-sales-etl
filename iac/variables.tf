variable "aws_region" {
  default = "us-east-1"
}

variable "lambda_function_name" {
  default = "Xls2Csv"
}

variable "key_pair_name" {
  default = "ec2_key_pair"
}

variable "airflow_subnet_id" {
  default = "subnet-02ba721d1f4017398"
}

variable "vpc_id" {
  default = "vpc-08ac55c9b8d00ece9"
}
