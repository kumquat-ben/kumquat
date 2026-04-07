resource "helm_release" "ingress_nginx" {
  name             = "ingress-nginx"
  repository       = "https://kubernetes.github.io/ingress-nginx"
  chart            = "ingress-nginx"
  namespace        = "ingress-nginx"
  create_namespace = true
  version          = "4.11.1"

  values = [yamlencode({
    controller = {
      ingressClassResource = {
        default = true
        name    = "nginx"
      }
      kind = "DaemonSet"
      service = {
        type = "NodePort"
        nodePorts = {
          http  = 32080
          https = 32443
        }
      }
      metrics = {
        enabled = true
      }
      config = {
        use-forwarded-headers = "true"
      }
    }
  })]
}
