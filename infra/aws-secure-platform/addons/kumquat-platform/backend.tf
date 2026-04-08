terraform {
  backend "s3" {
    key          = "aws-secure-platform/addons/kumquat-platform/terraform.tfstate"
    region       = "us-west-2"
    encrypt      = true
    use_lockfile = true
  }
}
