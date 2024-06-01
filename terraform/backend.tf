terraform {
  backend "gcs" {
    bucket = "tesietldomenico-terraform"
    prefix = "dev/state"
  }
}