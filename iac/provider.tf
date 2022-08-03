provider "aws" {
  region = var.aws_region
}

# Centralizar o arquivo de controle de estado do terraform
terraform {
  backend "s3" {
    bucket = "anp-tf-state"
    key    = "terraform.tfstate"
    region = "us-east-1"
  }
}