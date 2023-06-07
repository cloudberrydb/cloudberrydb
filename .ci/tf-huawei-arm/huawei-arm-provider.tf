variable "huawei_access_key" {
  sensitive = true
  type = string
}

variable "huawei_secret_key" {
  sensitive = true
  type = string
}

variable "huawei_region" {
  default = "cn-north-4"
}

variable "instance_name" {
  default = "ci"
}

variable "instance_image" {
  default = "660d40ec-2dc6-44b7-9f4d-7ae5bdf5319a"
}

variable "flavor_id" {
  default = "kc1.2xlarge.2"
}

variable "instance_securitygroup" {
  default = [
    "62555a74-5dcb-4fe3-8763-2c4d835b73c8"]
}

variable "instance_availability_zone" {
  default = "cn-north-4b"
}

variable "instance_os_disk_size" {
  default = 100
}

variable "instance_vxnet" {
  default = "eb86da07-01a2-40d5-8c45-6f38ae389592"
}

variable "instance_keypair" {
  default = "build"
}

terraform {
  required_providers {
    huaweicloud = {
      source = "huaweicloud/huaweicloud"
      version = "1.25.0"
    }
    ansible = {
      source = "nbering/ansible"
      version = "1.0.4"
    }
  }

  backend "local" {
  }
}

provider "huaweicloud" {
  access_key = var.huawei_access_key
  secret_key = var.huawei_secret_key
  region = var.huawei_region
}

resource "huaweicloud_compute_instance" "ci" {
  name = var.instance_name
  image_id = var.instance_image
  flavor_id = var.flavor_id
  security_group_ids = var.instance_securitygroup
  key_pair = var.instance_keypair
  availability_zone = var.instance_availability_zone
  system_disk_size = var.instance_os_disk_size

  network {
    uuid = var.instance_vxnet
  }
}

resource "ansible_host" "ci" {
  inventory_hostname = huaweicloud_compute_instance.ci.access_ip_v4
  groups = [
    "runner"]
  vars = {
    ansible_user = "root"
  }
}
