# Kumquat Blockchain Add-on

This add-on installs a stateful Kumquat blockchain node workload into k3s using Terraform and Helm.

It creates:

- the target Kubernetes namespace
- an optional EBS-backed `StorageClass` for blockchain node data
- a Helm release that deploys a `StatefulSet`
- one persistent volume claim per pod via `volumeClaimTemplates`

Each pod keeps its own `/data/kumquat` volume so a restarted or rescheduled node retains its local RocksDB state.

If the cluster already has storage provisioned, you can skip `StorageClass` creation and point the release at:

- an existing `StorageClass` through `existing_storage_class_name`
- an existing PVC through `existing_pvc_name`

When `existing_pvc_name` is set, the release mounts that claim directly and `replica_count` must stay `1`.

## Usage

```bash
cd infra/aws-secure-platform/addons/kumquat-blockchain
cp backend.hcl.example backend.hcl
cp terraform.tfvars.example terraform.tfvars
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

Override later as needed for:

- bootstrap peers
- network mode
- replica count
- mining settings
- genesis accounts
- service exposure
- existing PVC or storage class reuse
