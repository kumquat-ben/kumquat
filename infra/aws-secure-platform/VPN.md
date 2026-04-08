# AWS Client VPN Guide

This guide documents how operators connect to the private Kumquat k3s control plane in AWS.

Use this when `kubectl`, Helm, or Terraform add-on commands need access to the private cluster API.

## Why The VPN Exists

The production k3s API is intentionally private-only.

- Cluster endpoint: `kumquat-production-api-a26a03b163798460.elb.us-west-2.amazonaws.com`
- Client VPN endpoint ID: `cvpn-endpoint-08962574330901031`
- AWS region: `us-west-2`

The Terraform wiring for this lives in:

- [modules/vpn/main.tf](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform/modules/vpn/main.tf)
- [environments/production/main.tf](/Users/armenmerikyan/Desktop/wd/kumquat/infra/aws-secure-platform/environments/production/main.tf)

The VPN uses:

- AWS Client VPN
- certificate authentication
- `udp` on port `443`
- split tunnel enabled

## Required Local Files

This repo already contains local certificate material under `.local/aws-secure-platform/certs/`:

- [.local/aws-secure-platform/certs/vpn-ca.crt](/Users/armenmerikyan/Desktop/wd/kumquat/.local/aws-secure-platform/certs/vpn-ca.crt)
- [.local/aws-secure-platform/certs/vpn-client.crt](/Users/armenmerikyan/Desktop/wd/kumquat/.local/aws-secure-platform/certs/vpn-client.crt)
- [.local/aws-secure-platform/certs/vpn-client.key](/Users/armenmerikyan/Desktop/wd/kumquat/.local/aws-secure-platform/certs/vpn-client.key)

Treat the client key as sensitive.

## Export The VPN Profile

Run:

```bash
aws ec2 export-client-vpn-client-configuration \
  --region us-west-2 \
  --client-vpn-endpoint-id cvpn-endpoint-08962574330901031 \
  --query 'ClientConfiguration' \
  --output text > .local/aws-secure-platform/kumquat-production.ovpn
```

The exported profile should include:

- `remote cvpn-endpoint-08962574330901031.prod.clientvpn.us-west-2.amazonaws.com 443`
- `proto udp`
- `verify-x509-name vpn.kumquat.internal name`

## Add Client Certificate Material

You can either reference local files from the `.ovpn` profile or embed the certificate and key directly.

If your client supports file references, add:

```ovpn
ca /absolute/path/to/.local/aws-secure-platform/certs/vpn-ca.crt
cert /absolute/path/to/.local/aws-secure-platform/certs/vpn-client.crt
key /absolute/path/to/.local/aws-secure-platform/certs/vpn-client.key
```

If your client expects embedded material, append:

```ovpn
<cert>
...contents of vpn-client.crt...
</cert>
<key>
...contents of vpn-client.key...
</key>
```

Do not commit the generated `.ovpn` file if it contains embedded private key material.

## Connect

Use one of:

1. AWS VPN Client
2. OpenVPN Connect
3. Another OpenVPN-compatible client that supports AWS Client VPN profiles

Import `.local/aws-secure-platform/kumquat-production.ovpn`, then connect.

## Verify Access

After connecting, verify cluster reachability:

```bash
KUBECONFIG=/Users/armenmerikyan/Desktop/wd/kumquat/.local/aws-secure-platform/kubeconfig-production \
kubectl get nodes
```

You should also be able to resolve the private API endpoint:

```bash
nslookup kumquat-production-api-a26a03b163798460.elb.us-west-2.amazonaws.com
```

## Common Failure Modes

`kubectl` hangs:

- VPN is not connected
- the wrong kubeconfig is in use
- DNS is not resolving the private API endpoint over the VPN path

VPN connects but the cluster is still unreachable:

- verify the Client VPN endpoint ID matches production
- verify the exported profile was generated in `us-west-2`
- verify the client certificate and key match the root CA configured in Terraform

AWS export command fails:

- use `--region us-west-2`
- verify the endpoint exists with:

```bash
aws ec2 describe-client-vpn-endpoints --region us-west-2
```

## Operator Note

The k3s API is not meant to be reachable directly from the public internet. Future deployment work should assume the VPN is a prerequisite unless the work is being run from a trusted network path inside the VPC.
