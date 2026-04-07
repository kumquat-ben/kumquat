locals {
  name = "${var.project_name}-${var.environment}"

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Stack       = "aws-secure-platform"
    },
    var.tags,
  )
}

module "audit" {
  source = "../../modules/audit"

  name               = local.name
  aws_region         = var.aws_region
  vpc_id             = module.network.vpc_id
  flow_log_iam_scope = module.network.vpc_arn
  tags               = local.tags
}

module "network" {
  source = "../../modules/network"

  name                  = local.name
  aws_region            = var.aws_region
  availability_zones    = var.availability_zones
  vpc_cidr              = var.vpc_cidr
  public_subnet_cidrs   = var.public_subnet_cidrs
  private_subnet_cidrs  = var.private_subnet_cidrs
  isolated_subnet_cidrs = var.isolated_subnet_cidrs
  vpn_client_cidr       = var.vpn_client_cidr
  tags                  = local.tags
}

module "ecr" {
  source = "../../modules/ecr"

  name         = local.name
  repositories = var.ecr_repositories
  tags         = local.tags
}

module "k3s" {
  source = "../../modules/k3s"

  name                      = local.name
  aws_region                = var.aws_region
  vpc_id                    = module.network.vpc_id
  vpc_cidr                  = var.vpc_cidr
  private_subnet_ids        = module.network.private_subnet_ids
  server_subnet_indexes     = var.k3s_server_subnet_indexes
  vpn_client_cidr           = var.vpn_client_cidr
  api_tls_certificate_arn   = var.private_ingress_acm_certificate_arn
  allowed_app_ingress_cidrs = var.allowed_app_ingress_cidrs
  server_instance_type      = var.k3s_server_instance_type
  worker_instance_type      = var.k3s_worker_instance_type
  worker_min_size           = var.k3s_worker_min_size
  worker_desired_size       = var.k3s_worker_desired_size
  worker_max_size           = var.k3s_worker_max_size
  ssh_key_name              = var.ssh_key_name
  tags                      = local.tags
}

module "vpn" {
  source = "../../modules/vpn"

  name                             = local.name
  vpc_id                           = module.network.vpc_id
  vpc_cidr                         = var.vpc_cidr
  associated_subnet_ids            = slice(module.network.private_subnet_ids, 0, 2)
  client_cidr_block                = var.vpn_client_cidr
  server_certificate_arn           = var.client_vpn_server_certificate_arn
  root_certificate_chain_arn       = var.client_vpn_root_certificate_chain_arn
  cloudwatch_log_retention_in_days = 90
  tags                             = local.tags
}
