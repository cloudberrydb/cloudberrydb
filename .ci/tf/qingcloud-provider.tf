variable "qingcloud_access_key" {
  sensitive = true
  type      = string
}

variable "qingcloud_secret_key" {
  sensitive = true
  type      = string
}

variable "qingcloud_zone" {
  default = "pek3c"
}

variable "instance_name" {
  default = "ci"
}

variable "instance_image" {
  default = "img-96mbphfy"
}

variable "instance_class" {
  default = 202
}

variable "instance_cpu" {
  default = 8
}

variable "instance_memory" {
  default = 16384
}

variable "instance_os_disk_size" {
  default = 100
}

variable "instance_vxnet" {
  default = "vxnet-5tjdylj"
}

variable "instance_keypair" {
  default = [
  "kp-o07unn26"]
}

terraform {
  required_providers {
    qingcloud = {
      source  = "HashDataInc/qingcloud"
      version = "1.2.7"
    }
    ansible = {
      source  = "nbering/ansible"
      version = "1.0.4"
    }
  }
}

provider "qingcloud" {
  access_key = var.qingcloud_access_key
  secret_key = var.qingcloud_secret_key
  zone       = var.qingcloud_zone
}


resource "qingcloud_instance" "ci" {
  name             = var.instance_name
  image_id         = var.instance_image
  instance_class   = var.instance_class
  cpu              = var.instance_cpu
  memory           = var.instance_memory
  os_disk_size     = var.instance_os_disk_size
  managed_vxnet_id = var.instance_vxnet
  keypair_ids      = var.instance_keypair
}

resource "ansible_host" "ci" {
  inventory_hostname = qingcloud_instance.ci.private_ip
  groups = [
  "runner"]
  vars = {
    ansible_user = "root"
  }
}
