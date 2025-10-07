from airflow.utils.email import send_email
from pytz import timezone

RECIPIENTS = ["data.alerte@iuct-oncopole.fr"]


def notify_success(context):
    from airflow.utils.email import send_email
    from pytz import timezone

    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    formatted_date = execution_date.astimezone(timezone('Europe/Paris')).strftime('%Y-%m-%d %H:%M:%S')

    subject = f"✅ [SUCCÈS] Tâche {task_id} du DAG {dag_id}"

    body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #e6ffed; padding: 20px;">
        <div style="border-left: 5px solid #28a745; padding-left: 15px;">
          <h2 style="color: #28a745;"> Succès de la tâche Airflow</h2>
          <p><strong style="color: #000;">Tâche :</strong> {task_id}</p>
          <p><strong style="color: #000;">DAG :</strong> {dag_id}</p>
          <p><strong style="color: #000;">Date d’exécution (heure France) :</strong> {formatted_date}</p>
        </div>
      </body>
    </html>
    """

    send_email(to=RECIPIENTS, subject=subject, html_content=body)


def notify_failure(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    formatted_date = execution_date.astimezone(timezone('Europe/Paris')).strftime('%Y-%m-%d %H:%M:%S')

    subject = f"❌ [ÉCHEC] Tâche {task_id} du DAG {dag_id}"

    body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #fff3f3; padding: 20px;">
        <div style="border-left: 5px solid #d9534f; padding-left: 15px;">
          <h2 style="color: #d9534f;"> Échec de la tâche Airflow</h2>
          <p><strong style="color: #000;">Tâche :</strong> {task_id}</p>
          <p><strong style="color: #000;">DAG :</strong> {dag_id}</p>
          <p><strong style="color: #000;">Date d’exécution (heure France) :</strong> {formatted_date}</p>
          <p><strong style="color: #000;">Erreur :</strong><br>
            <pre style="background-color: #f8d7da; padding: 10px; border-radius: 5px; color: #721c24;">
{str(exception)}
            </pre>
          </p>
        </div>
      </body>
    </html>
    """

    send_email(to=RECIPIENTS, subject=subject, html_content=body)
