output "vpc_id" {
  value = aws_vpc.this.id
}

output "vpc_arn" {
  value = aws_vpc.this.arn
}

output "private_subnet_ids" {
  value = values(aws_subnet.private)[*].id
}

output "public_subnet_ids" {
  value = values(aws_subnet.public)[*].id
}

output "isolated_subnet_ids" {
  value = values(aws_subnet.isolated)[*].id
}
