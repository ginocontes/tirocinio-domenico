terraform { 
    required_version = ">= 1.0.0" 
    required_providers { 
        google = { 
            source = "hashicorp/google" 
            version = ">= 4.0.0" 
            } 

        google-beta = { 
            source = "hashicorp/google-beta" 
            version = ">= 4.0.0" 
            }
    }
}

provider "google" {
	credentials = "tf-sa.json"
	project = var.project_id
	region = var.region
	zone = var.zone
}

provider "google-beta" {
	credentials = "tf-sa.json"
	project = var.project_id
	region = var.region
	zone = var.zone
}
