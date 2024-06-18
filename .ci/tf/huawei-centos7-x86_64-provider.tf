variable "huawei_access_key_2" {
  sensitive = true
  type      = string
}

variable "huawei_secret_key_2" {
  sensitive = true
  type      = string
}

variable "huawei_region" {
  default = "cn-north-4"
}

variable "instance_name" {
  default = "ci"
}

variable "instance_image" {
  # releng-base-cbdb-build-centos7-x86_64-20240527
  default = "2efb1afa-a01f-4ce0-8bbc-ad16afd97e72"
}

variable "flavor_id" {
  default = "m6.4xlarge.8"
}

variable "instance_securitygroup" {
  default = [
    "653629ee-805e-431e-827d-349de18dc767"
  ]
}

variable "instance_availability_zone" {
  default = "cn-north-4b"
}

variable "instance_os_disk_size" {
  default = 200
}

variable "instance_vxnet" {
  default = "95c77d8f-3291-4db3-86c2-30e02e1eab9b"
}

variable "instance_keypair" {
  default = "build"
}

terraform {
  required_providers {
    huaweicloud = {
      source  = "huaweicloud/huaweicloud"
      version = "1.25.0"
    }
    ansible = {
      source  = "nbering/ansible"
      version = "1.0.4"
    }
  }

  backend "local" {
  }
}

variable "enterprise_project_id" {
  default = "1bba1faa-f3fe-438c-b3fd-073e4044f924"
}

provider "huaweicloud" {
  access_key = var.huawei_access_key_2
  secret_key = var.huawei_secret_key_2
  region     = var.huawei_region
  enterprise_project_id = var.enterprise_project_id
}

resource "huaweicloud_compute_instance" "ci" {
  name               = var.instance_name
  image_id           = var.instance_image
  flavor_id          = var.flavor_id
  security_group_ids = var.instance_securitygroup
  key_pair           = var.instance_keypair
  availability_zone  = var.instance_availability_zone
  system_disk_size   = var.instance_os_disk_size

  network {
    uuid = var.instance_vxnet
  }
}

resource "ansible_host" "ci" {
  inventory_hostname = huaweicloud_compute_instance.ci.access_ip_v4
  groups = [
    "runner"
  ]
  vars = {
    ansible_user = "root"
  }
}
