
# Introduzione al progetto

Nei capitoli precedenti si è descritto il contesto tecnologico del settore BigData analitico enterprise, con particolare enfasi sulle tecnologie Google Cloud abilitanti.
Spaziando dai sistemi di archiviazioni di dati transazionali e analitici, analizzando le loro differenze; l'evoluzione di questi ultimi nei moderni DataWarehouses su tecnologie Cloud, come questi datawarehouse implementano trasformazioni massive del dato tramite processi di ETL/ELT e le tecnologie Google Cloud Native abilitanti: BigQuery, Composer.
La mia esperienza di tirocinio in Accenture S.p.a mi ha aperto la possibilità di sperimentare, seppur brevemente, con un cliente complesso (Fortune 500 Glboal) nel settore finanziario (da qui in poi la "Banca").
I sistemi informativi bancari sono estremamente intrecciati .
La Banca usa Salesforce, ad oggi il  Software as a Service CRM (Customer Relationship Management) più utilizzato al mondo. 
Un Customer Relationship Management Service è uno strumento fondamentale per le aziende moderne, soprattutto di grandi dimensioni. Guidano infatti l'azienda in tutto il processo di gestione del cliente, dall'acquisizione fino all'analisi del "churn rate" (quanti clienti abbandonano i servizi), offrendo servizi di analisi integrati offerte tramite comode web UIs.

Una parte importante per la Banca è la gestione delle campagne di Marketing, per cui utilizzano Salesforce Marketing Cloud.
Salesforce è estremamente abile nella creazione e gestione di campagne di Marketing. 
Nella banca centinaia di sistemi software devono essere integrati tra di loro, alcuni moderni come Google Cloud, altri legacy, sviluppati da fornitori esterni decine di anni fa con tecnologie ormai obsolete, ma critici per l'operatività del business.
Uno di questi sistemi (un software rendicontativo) ha la necessità di importare csv in un determinato formato diverso da quello di Marketing Cloud. 
Salesforce offre "Salesforce Automation" per implementare processi di trasformazione in SQL che attingono ai dati in Marketing Cloud ed esportano CSV. Ma soffre di una scalabilità in termini di Gigabyte elaborati molto meno performante rispetto a una soluzione Cloud come BigQuery. Di fatto il batch notturno cominciava a ritardare di molto i "cutoff" stabiliti dal sistema rendicontativo, alcune volte fallendo il processo.

Il progetto consisteva nella riscrittura di questo processo usando le tecnologie Google Cloud.
Sostituendo Salesforce Automation con un ELT orchestrato tramite Composer che prende i file da Salesforce Marketing Cloud spostandoli su Google Cloud Storage, li ignetta su un dataset BigQuery, li elabora nel formato richiesto dal sistema rendicontativo, esportando le tabelle finali BigQuery in file CSV su Cloud Storage.
Per ovvi motivi di diritti di proprietà intellettuale mostrerò una procedura semplificata su dati anonimizzati, ma che, nonostante implementano le idee ad Alto livello del progetto originario.

## Terraform


## Airflow


## Composer


# Conclusioni


