### Google Cloud Pipeline Repo

Using Apache Beam's SDK to load, clean and upload data into a Cloud SQL (PostgreSQL) server.


Changelog:
(2022-12-20) - Changed code to handle credentials better.  

To future Alex:
The pipeline's insert only and doesn't move files from bucket to bucket.  May want to do that if you ever feel like over-engineering the heck out of this...

Resources:
- Loading data into bigquery by SadeeqAkintola
-- https://medium.com/@SadeeqAkintola/loading-data-from-multiple-csv-files-in-gcs-into-bigquery-using-cloud-dataflow-python-a695648e9c63
-- https://github.com/SadeeqAkintola/data-rivers

- Best Practices by Pablo Estrada
-- https://www.youtube.com/watch?v=Cf3-Z_HQRdE
-- https://github.com/pabloem/beam-covid-example/tree/a155c9d8c25327efcaa56ab0fdd3ce4869318edb

- Other
-- https://medium.com/posh-engineering/how-to-deploy-your-apache-beam-pipeline-in-google-cloud-dataflow-3b9fe431c7bb