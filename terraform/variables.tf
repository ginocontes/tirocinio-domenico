variable zone {
  description = "Zone"
  type = string
  default = "europe-west1-b"
}

variable project_id {
  description = "Project id"
  type = string
}
variable project_number {
  description = "Project number"
  type = string
}

variable "region" {
  description = "The region for resources and networking"
  type = string
  default = "europe-west1"
}

variable "composer_image_version" {
  description = "Composer version"
  type = string
}



variable "sa_namespace" {
  description = "k8s namespace of k8s service account"
  type = string
}

variable "podoperator_sa" {
  description = "k8s sa used by workload identity"
  type = string
}

variable environment {
  description = "Environment to deploy the infrastructure"
  type = string
}