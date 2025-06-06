# Déploiement du pipeline Airflow

## Dossier projet

Le projet est installé dans :  
`/home/administrateur/airflow/`

Airflow utilise :
- un environnement virtuel : `airflow_env`
- un dossier `dags/` contenant les fichiers ETL et utilitaires

---

## Configuration des services systemd

### Fichier `airflow-webserver.service`

```ini
[Unit]
Description=Airflow Webserver
After=network.target

[Service]
Environment="PATH=/home/administrateur/airflow_env/bin"
Environment="LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7"
WorkingDirectory=/home/administrateur/airflow
ExecStart=/home/administrateur/airflow_env/bin/airflow webserver
Restart=always
User=administrateur

[Install]
WantedBy=multi-user.target
```
### Fichier `airflow-scheduler.service`

```ini
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
Environment="PATH=/home/administrateur/airflow_env/bin"
Environment="LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7"
WorkingDirectory=/home/administrateur/airflow
ExecStart=/home/administrateur/airflow_env/bin/airflow scheduler
Restart=always
User=administrateur

[Install]
WantedBy=multi-user.target
```
---

## Activation

```bash
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler
```

## Vérification
```bash
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler
```
### Vérifier aussi
```bash
curl http://10.210.22.130:8082
```
---

## 💡 Astuce
Si cx_Oracle génère une erreur liée à libnnz.so :

-  Vérifier le chemin de LD_LIBRARY_PATH
-  Ajoutez-le dans systemd (cf. ci-dessus)

