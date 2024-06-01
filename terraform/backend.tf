terraform {
  backend "gcs" {
    bucket = "tesielt-terraform"
    prefix = "dev/state"
  }
}