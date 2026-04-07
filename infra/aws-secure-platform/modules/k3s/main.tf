data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

locals {
  server_subnet_ids = [for idx in var.server_subnet_indexes : var.private_subnet_ids[idx]]
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_kms_key" "k3s" {
  description             = "KMS key for k3s secrets in ${var.name}"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name = "${var.name}-k3s-kms"
  })
}

resource "aws_kms_alias" "k3s" {
  name          = "alias/${var.name}-k3s"
  target_key_id = aws_kms_key.k3s.key_id
}

resource "random_password" "cluster_token" {
  length  = 48
  special = false
}

resource "aws_ssm_parameter" "cluster_token" {
  name   = "/${var.name}/k3s/cluster-token"
  type   = "SecureString"
  value  = random_password.cluster_token.result
  key_id = aws_kms_key.k3s.arn

  tags = var.tags
}

data "aws_iam_policy_document" "instance_assume" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "server" {
  name               = "${var.name}-k3s-server-role"
  assume_role_policy = data.aws_iam_policy_document.instance_assume.json
}

resource "aws_iam_role" "worker" {
  name               = "${var.name}-k3s-worker-role"
  assume_role_policy = data.aws_iam_policy_document.instance_assume.json
}

resource "aws_iam_role_policy_attachment" "server_ssm" {
  role       = aws_iam_role.server.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "worker_ssm" {
  role       = aws_iam_role.worker.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "cluster_token_access" {
  statement {
    effect = "Allow"
    actions = [
      "ssm:GetParameter",
    ]
    resources = [aws_ssm_parameter.cluster_token.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "kms:Decrypt",
    ]
    resources = [aws_kms_key.k3s.arn]
  }
}

resource "aws_iam_role_policy" "server_cluster_token" {
  name   = "${var.name}-server-cluster-token"
  role   = aws_iam_role.server.id
  policy = data.aws_iam_policy_document.cluster_token_access.json
}

resource "aws_iam_role_policy" "worker_cluster_token" {
  name   = "${var.name}-worker-cluster-token"
  role   = aws_iam_role.worker.id
  policy = data.aws_iam_policy_document.cluster_token_access.json
}

resource "aws_iam_instance_profile" "server" {
  name = "${var.name}-k3s-server-profile"
  role = aws_iam_role.server.name
}

resource "aws_iam_instance_profile" "worker" {
  name = "${var.name}-k3s-worker-profile"
  role = aws_iam_role.worker.name
}

resource "aws_security_group" "server" {
  name        = "${var.name}-k3s-server-sg"
  description = "Security group for k3s server nodes."
  vpc_id      = var.vpc_id

  ingress {
    description = "Kubernetes API from VPN clients"
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = [var.vpn_client_cidr]
  }

  ingress {
    description = "Kubernetes API from private VPC ranges"
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "k3s supervisor traffic from private VPC ranges"
    from_port   = 9345
    to_port     = 9345
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "etcd peer traffic between server nodes"
    from_port   = 2379
    to_port     = 2380
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Flannel overlay between cluster nodes"
    from_port   = 8472
    to_port     = 8472
    protocol    = "udp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "Node and pod traffic within the cluster"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Private east-west traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Controlled HTTPS egress via NAT or VPC endpoints"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "NTP to Amazon Time Sync"
    from_port   = 123
    to_port     = 123
    protocol    = "udp"
    cidr_blocks = ["169.254.169.123/32"]
  }

  tags = merge(var.tags, {
    Name = "${var.name}-k3s-server-sg"
  })
}

resource "aws_security_group" "worker" {
  name        = "${var.name}-k3s-worker-sg"
  description = "Security group for k3s worker nodes."
  vpc_id      = var.vpc_id

  ingress {
    description = "Kubernetes API and supervisor from private VPC ranges"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "Flannel overlay between cluster nodes"
    from_port   = 8472
    to_port     = 8472
    protocol    = "udp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description     = "Ingress traffic from the internal ALB"
    from_port       = 32080
    to_port         = 32443
    protocol        = "tcp"
    security_groups = [aws_security_group.app_alb.id]
  }

  egress {
    description = "Private east-west traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Controlled HTTPS egress via NAT or VPC endpoints"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "NTP to Amazon Time Sync"
    from_port   = 123
    to_port     = 123
    protocol    = "udp"
    cidr_blocks = ["169.254.169.123/32"]
  }

  tags = merge(var.tags, {
    Name = "${var.name}-k3s-worker-sg"
  })
}

resource "aws_lb" "api" {
  name               = substr("${var.name}-api", 0, 32)
  internal           = true
  load_balancer_type = "network"
  subnets            = var.private_subnet_ids

  enable_deletion_protection = true

  tags = merge(var.tags, {
    Name = "${var.name}-api"
  })
}

resource "aws_lb_target_group" "api" {
  name        = substr("${var.name}-api-tg", 0, 32)
  port        = 6443
  protocol    = "TCP"
  vpc_id      = var.vpc_id
  target_type = "instance"

  health_check {
    enabled  = true
    protocol = "TCP"
    port     = "6443"
  }

  tags = merge(var.tags, {
    Name = "${var.name}-api-tg"
  })
}

resource "aws_lb_listener" "api" {
  load_balancer_arn = aws_lb.api.arn
  port              = 6443
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

resource "aws_launch_template" "server" {
  name_prefix            = "${var.name}-k3s-server-"
  image_id               = data.aws_ami.al2023.id
  instance_type          = var.server_instance_type
  key_name               = var.ssh_key_name
  update_default_version = true

  iam_instance_profile {
    name = aws_iam_instance_profile.server.name
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_protocol_ipv6          = "disabled"
    http_put_response_hop_limit = 2
    http_tokens                 = "required"
    instance_metadata_tags      = "disabled"
  }

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      encrypted             = true
      delete_on_termination = true
      volume_size           = 40
      volume_type           = "gp3"
    }
  }

  vpc_security_group_ids = [aws_security_group.server.id]

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name = "${var.name}-k3s-server"
      Role = "k3s-server"
    })
  }
}

resource "aws_instance" "server" {
  count = 3

  ami                         = data.aws_ami.al2023.id
  instance_type               = var.server_instance_type
  subnet_id                   = local.server_subnet_ids[count.index]
  iam_instance_profile        = aws_iam_instance_profile.server.name
  key_name                    = var.ssh_key_name
  vpc_security_group_ids      = [aws_security_group.server.id]
  associate_public_ip_address = false
  ebs_optimized               = true

  metadata_options {
    http_endpoint               = "enabled"
    http_protocol_ipv6          = "disabled"
    http_put_response_hop_limit = 2
    http_tokens                 = "required"
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    encrypted   = true
    volume_size = 40
    volume_type = "gp3"
  }

  user_data = base64encode(templatefile("${path.module}/templates/server-userdata.sh.tftpl", {
    aws_region         = var.aws_region
    cluster_token_name = aws_ssm_parameter.cluster_token.name
    api_endpoint       = aws_lb.api.dns_name
    mode               = count.index == 0 ? "init" : "join"
  }))

  tags = merge(var.tags, {
    Name = "${var.name}-k3s-server-${count.index + 1}"
    Role = "k3s-server"
  })
}

resource "aws_lb_target_group_attachment" "api_servers" {
  count = length(aws_instance.server)

  target_group_arn = aws_lb_target_group.api.arn
  target_id        = aws_instance.server[count.index].id
  port             = 6443
}

resource "aws_launch_template" "worker" {
  name_prefix            = "${var.name}-k3s-worker-"
  image_id               = data.aws_ami.al2023.id
  instance_type          = var.worker_instance_type
  key_name               = var.ssh_key_name
  update_default_version = true

  iam_instance_profile {
    name = aws_iam_instance_profile.worker.name
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_protocol_ipv6          = "disabled"
    http_put_response_hop_limit = 2
    http_tokens                 = "required"
    instance_metadata_tags      = "disabled"
  }

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      encrypted             = true
      delete_on_termination = true
      volume_size           = 60
      volume_type           = "gp3"
    }
  }

  vpc_security_group_ids = [aws_security_group.worker.id]

  user_data = base64encode(templatefile("${path.module}/templates/worker-userdata.sh.tftpl", {
    aws_region         = var.aws_region
    cluster_token_name = aws_ssm_parameter.cluster_token.name
    api_endpoint       = aws_lb.api.dns_name
  }))

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name = "${var.name}-k3s-worker"
      Role = "k3s-worker"
    })
  }
}

resource "aws_autoscaling_group" "worker" {
  name                      = "${var.name}-k3s-workers"
  min_size                  = var.worker_min_size
  desired_capacity          = var.worker_desired_size
  max_size                  = var.worker_max_size
  health_check_type         = "EC2"
  health_check_grace_period = 300
  vpc_zone_identifier       = var.private_subnet_ids
  force_delete              = false

  launch_template {
    id      = aws_launch_template.worker.id
    version = aws_launch_template.worker.latest_version
  }

  tag {
    key                 = "Name"
    value               = "${var.name}-k3s-worker"
    propagate_at_launch = true
  }

  tag {
    key                 = "Role"
    value               = "k3s-worker"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_policy" "worker_cpu" {
  name                   = "${var.name}-workers-target-cpu"
  autoscaling_group_name = aws_autoscaling_group.worker.name
  policy_type            = "TargetTrackingScaling"

  target_tracking_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ASGAverageCPUUtilization"
    }

    target_value = 60
  }
}

resource "aws_security_group" "app_alb" {
  name        = "${var.name}-app-alb-sg"
  description = "Security group for the private application ALB."
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTP from approved private CIDRs"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = length(var.allowed_app_ingress_cidrs) > 0 ? var.allowed_app_ingress_cidrs : [var.vpc_cidr, var.vpn_client_cidr]
  }

  ingress {
    description = "HTTPS from approved private CIDRs"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = length(var.allowed_app_ingress_cidrs) > 0 ? var.allowed_app_ingress_cidrs : [var.vpc_cidr, var.vpn_client_cidr]
  }

  egress {
    description = "Forward traffic to worker nodes"
    from_port   = 32080
    to_port     = 32443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  tags = merge(var.tags, {
    Name = "${var.name}-app-alb-sg"
  })
}

resource "aws_lb" "app" {
  name               = substr("${var.name}-apps", 0, 32)
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.app_alb.id]
  subnets            = var.private_subnet_ids

  enable_deletion_protection = true

  tags = merge(var.tags, {
    Name = "${var.name}-apps"
  })
}

resource "aws_lb_target_group" "app_http" {
  name        = substr("${var.name}-app-http", 0, 32)
  port        = 32080
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "instance"

  health_check {
    enabled             = true
    path                = "/"
    matcher             = "404"
    protocol            = "HTTP"
    port                = "32080"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 15
  }

  tags = merge(var.tags, {
    Name = "${var.name}-app-http"
  })
}

resource "aws_autoscaling_attachment" "app_http" {
  autoscaling_group_name = aws_autoscaling_group.worker.name
  lb_target_group_arn    = aws_lb_target_group.app_http.arn
}

resource "aws_lb_listener" "app_http" {
  load_balancer_arn = aws_lb.app.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = var.api_tls_certificate_arn == null ? "forward" : "redirect"

    dynamic "redirect" {
      for_each = var.api_tls_certificate_arn == null ? [] : [1]
      content {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }

    dynamic "forward" {
      for_each = var.api_tls_certificate_arn == null ? [1] : []
      content {
        target_group {
          arn = aws_lb_target_group.app_http.arn
        }
      }
    }
  }
}

resource "aws_lb_listener" "app_https" {
  count = var.api_tls_certificate_arn == null ? 0 : 1

  load_balancer_arn = aws_lb.app.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.api_tls_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.app_http.arn
  }
}
