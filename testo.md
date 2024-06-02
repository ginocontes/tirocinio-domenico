
# Introduzione al progetto

Nei capitoli precedenti si è descritto il contesto tecnologico del settore BigData analitico enterprise, con particolare enfasi sulle tecnologie Google Cloud abilitanti.
Spaziando dai sistemi di archiviazioni di dati transazionali e analitici, analizzando le loro differenze; l'evoluzione di questi ultimi nei moderni DataWarehouses su tecnologie Cloud, come questi datawarehouse implementano trasformazioni massive del dato tramite processi di ETL/ELT e le tecnologie Google Cloud Native abilitanti: BigQuery, Composer.
La mia esperienza di tirocinio in Accenture S.p.a mi ha aperto la possibilità di sperimentare, seppur brevemente, con un cliente complesso (Fortune 500 Glboal) nel settore finanziario (da qui in poi la "Banca").
I sistemi informativi bancari sono estremamente intrecciati .
La Banca usa Salesforce, ad oggi il  Software as a Service CRM (Customer Relationship Management) più utilizzato al mondo. 
Un Customer Relationship Management Service è uno strumento fondamentale per le aziende moderne, soprattutto di grandi dimensioni. Guidano infatti l'azienda in tutto il processo di gestione del cliente, dall'acquisizione fino all'analisi del "churn rate" (quanti clienti abbandonano i servizi), offrendo servizi di analisi integrati offerte tramite comode web UIs.

Una parte importante per la Banca è la gestione delle campagne di Marketing, per cui utilizzano "Salesforce Marketing Cloud".
Salesforce è estremamente abile nella creazione e gestione di campagne di Marketing. 
Nella banca centinaia di sistemi software devono essere integrati tra di loro, alcuni moderni come Google Cloud, altri legacy, sviluppati da fornitori esterni decine di anni fa con tecnologie ormai obsolete, ma critici per l'operatività del business.
Uno di questi sistemi (un software rendicontativo) ha la necessità di importare csv in un determinato formato diverso da quello di Marketing Cloud. 
Salesforce offre "Salesforce Automation" per implementare processi di trasformazione in SQL che attingono ai dati in Marketing Cloud ed esportano CSV. Ma soffre di una scalabilità in termini di Gigabyte elaborati molto meno performante rispetto a una soluzione Cloud come BigQuery. Di fatto il batch notturno cominciava a ritardare di molto i "cutoff" stabiliti dal sistema rendicontativo, alcune volte fallendo il processo.

Il progetto consiste nella riscrittura di questo processo usando le tecnologie Google Cloud.
Sostituendo Salesforce Automation con un ELT orchestrato tramite Composer che prende i file da Salesforce Marketing Cloud spostandoli su Google Cloud Storage, li inietta su un dataset BigQuery, li elabora nel formato richiesto dal sistema rendicontativo, esportando le tabelle finali BigQuery in file CSV su Cloud Storage.
Per ovvi motivi di diritti di proprietà intellettuale mostrerò una procedura semplificata su dati anonimizzati. Ciònonostante il software illustra ad alto livello le idee del progetto originario.

Nel corso della discussione mi soffermerò solamente sui pezzi di codice più importante del progetto, il codice completo è consultabile sulla repository pubblica github https://github.com/domenicodemarchis/tirocinio-domenico.

## Terraform
La gestione delle risorse progettuale è delegata a Terraform, introdotto nel capitolo 4.2.
Prima di definire l'infrastruttura bisogna definire dei parametri di configurazione che definiscono lo "stato di verità" dell'infrastruttura. In ambiente Google Cloud la "best practice" è di salvare lo stato di verità in un bucket su GCS, a cui il codice punta.
```terraform
terraform {
  backend "gcs" {
    bucket = "tesietldomenico-terraform"
    prefix = "dev/state"
  }
}
```
Terraform è specificatamente progettato per supportare un'architettura software a plugin, in cui ogni piattaforma come GCP, AWS o Azure può sviluppare providers (connettori) che permettono di definire risorse che rappresentano i servizi messi a disposizione. Google, ovviamente, è uno dei provider più supportati, virtualmente ogni risorsa e api di GCP viene implementata, in un modo o nell'altro, come risorsa del provider google. 

```terraform
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
```

## Airflow


## BigQuery

Nel capitolo 4 abbiamo analizzato a grandi linee l'architettura software fisica di un cloud Datawarehouse come BigQuery e come riesce a processare terabyte di dati senza difficoltà e senza la necessità di predisporre centinaia di server sulla propria infrastruttura, sfruttando le risorse computazionali di Google.
Sapere l'architettura distribuita e il livello di complessità in un prodotto come BigQuery ti permette di apprezzarne l'utilizzo da programmatore e analista, poichè le query vengono espresse in SQL, un linguaggio dichiarativo che si concentra sulla definizione e processing del livello logico del database, tralasciando i dettagli implementativi



# Conclusioni


