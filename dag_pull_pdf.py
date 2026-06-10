from datetime import datetime

from airflow import DAG
from airflow.operators.email import EmailOperator


DAG_ID = "rappel_pull_pdf_lakehouse"

default_args = {
    "owner": "DATAIA",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 12 * * 0",
    catchup=False,
    tags=["pdf", "lakehouse", "rappel"],
) as dag:

    send_reminder = EmailOperator(
        task_id="send_pull_pdf_reminder",
        to=["Gutkowski.Florian@iuct-oncopole.fr"],
        subject="Rappel - Extraction des PDF depuis le server lakehouse",
        html_content="""
            <p>Bonjour,</p>
            <p>
                Rappel : lancer l'extraction des PDF depuis le server lakehouse.
            </p>
            <p>
                Commande a lancer depuis le serveur Airflow :
                <br>
                <code>ssh -tt administrateur@srvlakehouse sudo bash /opt/pull_pdf.sh</code>
            </p>
        """,
    )

    send_reminder
