## locals
## insert schema here
locals {
  pubschema = jsonencode([
    { name = "data", type = "JSON"}
  ])

  last_publish_time_schema = jsonencode([
    { name = "stream_linear_source", type="TIMESTAMP"}
  ])

}


## Security and IAM

# service account used by  composer environment
module "composer_sa" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/iam-service-account"
  project_id = var.project_id
  name       = "composer-sa"
  iam_project_roles = {
    (var.project_id) = [
      "roles/editor",
      "roles/composer.user",
      "roles/composer.worker",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
      "roles/iam.serviceAccountUser",
      "roles/composer.ServiceAgentV2Ext"
    ]
  }
}

## necessary for composer deployment

resource "google_service_account_iam_member" "service_agent_extensions" {
  provider = google-beta
  service_account_id = module.composer_sa.id
  role = "roles/composer.ServiceAgentV2Ext"
  member = "serviceAccount:service-${var.project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}


## This is the service account that will be used by kubernetes pod operator to run the dbt jobs
module "dbt_sa" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/iam-service-account"
  project_id = var.project_id
  name       = "dbt-sa"
  iam_project_roles = {
    (var.project_id) = [
      "roles/editor"
    ]
  }
  iam = {
    "roles/iam.serviceAccountTokenCreator" = [ "serviceAccount:${var.project_number}@cloudbuild.gserviceaccount.com"]
  }
}


resource "google_compute_network" "vpc_network" {
  project = var.project_id
  name = "etl-tesi-${var.environment}-vpc"
  auto_create_subnetworks = false
  routing_mode = "GLOBAL"

}

resource "google_compute_subnetwork" "composersub" {
  project = var.project_id
  name = "composer-sub"
  network = google_compute_network.vpc_network.id
  region = "europe-west1"
  ip_cidr_range = "10.1.0.0/24"
  secondary_ip_range = [{
    range_name = "pods"
    ip_cidr_range = "172.16.0.0/20"
  },
  {
    range_name = "services"
    ip_cidr_range = "192.168.0.0/24"
  }
  ]
  private_ip_google_access =  true
}

resource "google_compute_subnetwork" "vmsubnet" {
  project = var.project_id
  region = "europe-west1"
  name = "vm-subnet"
  network = google_compute_network.vpc_network.id
  ip_cidr_range = "10.0.16.0/24"
}

module "default-nat" {
  source         = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/net-cloudnat"
  project_id     = var.project_id
  region         = var.region
  name           = "default-nat"
  router_network = google_compute_network.vpc_network.self_link
}


module "default_firewall" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/net-vpc-firewall"
  project_id   = var.project_id
  network      = google_compute_network.vpc_network.name
  default_rules_config = {
    admin_ranges = [google_compute_subnetwork.composersub.ip_cidr_range, google_compute_subnetwork.vmsubnet.ip_cidr_range]
  }
}


module "datavm" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/compute-vm"
  project_id = var.project_id
  zone       = var.zone
  name       = "datavm"
  network_interfaces = [{
    network    = google_compute_network.vpc_network.self_link
    subnetwork = google_compute_subnetwork.vmsubnet.self_link
  }]
  service_account_create = false
  service_account_scopes = ["cloud-platform"]
  tags = [ "ssh" ]
}





resource "google_composer_environment" "etl-tesi-composer" {
  provider =  google-beta
  name    = "etl-tesi-composer"
  region  = var.region
  project = var.project_id
  config {
    software_config {
      image_version  = var.composer_image_version
      env_variables = {
        AIRFLOW_VAR_BIGQUERY_LOCATION = var.region
       }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      worker {
        cpu = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }


    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {

      network         = google_compute_network.vpc_network.self_link
      subnetwork      = google_compute_subnetwork.composersub.self_link
      service_account = module.composer_sa.email
      
    }
  }
}



module "import_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/gcs"
  project_id = var.project_id
  name       = "etl-tesi-${var.environment}-data-ingest"
  versioning = true
  location = "europe-west1"
  storage_class = "REGIONAL"
  uniform_bucket_level_access = false
}



### BigQuery Datasets and tables with schema

# module "bigquery-dataset_gap_dataset_raw" {
#   source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric.git?ref=v23.0.0/modules/bigquery-dataset"
#   project_id = var.project_id
#   id         = "gap_dataset_raw"
#   location = var.region
#   iam = {
#     "roles/bigquery.dataEditor" = ["serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"] # Make pubsub default service account write to bigquery for the subscription
#     "roles/bigquery.admin" = [ module.dbt_sa.iam_email ] # Mak
#   }

#   tables = {
#     IT_LAST_PUBLISH_TIME = {
#       friendly_name = "Last published time"
#       labels = {}
#       options = null
#       partitioning = null
#       schema = local.last_publish_time_schema
#       deletion_protection = false
#     }
#     UK_LAST_PUBLISH_TIME = {
#       friendly_name = "Last published time"
#       labels = {}
#       options = null
#       partitioning = null
#       schema = local.last_publish_time_schema
#       deletion_protection = false
#     }

#     DE_LAST_PUBLISH_TIME = {
#       friendly_name = "Last published time"
#       labels = {}
#       options = null
#       partitioning = null
#       schema = local.last_publish_time_schema
#       deletion_protection = false
#     }

#     IT-DCM-STREAM_LINEAR = {
#       friendly_name = "Streaming start IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }

#     IT-DCM-STREAM_STOP = {
#       friendly_name = "Streaming stop IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }
#     IT-DCM-STREAM_VOD = {
#       friendly_name = "Streaming vod IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }
#     IT-DCM-HEARTBEAT = {
#       friendly_name = "Heartbeat info IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }

#     IT-ACCOUNT_MANAGER-ACCOUNT_UPDATED = {
#       friendly_name = "Account updated info IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     IT-IMS-PROFILE_CREATED = {
#       friendly_name = "profile created IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     IT-IMS-PROFILE_MODIFIED = {
#       friendly_name = "Profile modified IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     IT-PAYMENTS_MANAGER-STORE_PAYMENT_ACCOUNTS = {
#       friendly_name = "payment info IT"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }

#     IT-LOGINS = {
#       friendly_name = "login info"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }

#     # UK
#     UK-DCM-STREAM_LINEAR = {
#       friendly_name = "Streaming start UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }

#     UK-DCM-STREAM_STOP = {
#       friendly_name = "Streaming stop UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }
#     UK-DCM-STREAM_VOD = {
#       friendly_name = "Streaming vod UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }
#     UK-DCM-HEARTBEAT = {
#       friendly_name = "Heartbeat info UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }

#     UK-ACCOUNT_MANAGER-ACCOUNT_UPDATED = {
#       friendly_name = "Account updated info UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     UK-IMS-PROFILE_CREATED = {
#       friendly_name = "profile created UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     UK-IMS-PROFILE_MODIFIED = {
#       friendly_name = "Profile modified UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     UK-PAYMENTS_MANAGER-STORE_PAYMENT_ACCOUNTS = {
#       friendly_name = "payment info UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }

#     UK-LOGINS = {
#       friendly_name = "login info"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }

#     # DE
#     DE-DCM-STREAM_LINEAR = {
#       friendly_name = "Streaming start UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }

#     DE-DCM-STREAM_STOP = {
#       friendly_name = "Streaming stop UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }
#     DE-DCM-STREAM_VOD = {
#       friendly_name = "Streaming vod UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }
#     DE-DCM-HEARTBEAT = {
#       friendly_name = "Heartbeat info UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false
#     }

#     DE-ACCOUNT_MANAGER-ACCOUNT_UPDATED = {
#       friendly_name = "Account updated info UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     DE-IMS-PROFILE_CREATED = {
#       friendly_name = "profile created UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     DE-IMS-PROFILE_MODIFIED = {
#       friendly_name = "Profile modified UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }
#     DE-PAYMENTS_MANAGER-STORE_PAYMENT_ACCOUNTS = {
#       friendly_name = "payment info UK"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }      
#       schema = local.pubschema
#       deletion_protection = false    
#     }

#     DE-LOGINS = {
#       friendly_name = "login info"
#       labels = {}
#       options = null
#       partitioning = {
#         field = "publish_time"
#         range = null # use start/end/interval for range
#         time  = { type = "DAY", expiration_ms = null }
#       }  
#       schema = local.pubschema
#       deletion_protection = false    
#     }

#   }
# }


