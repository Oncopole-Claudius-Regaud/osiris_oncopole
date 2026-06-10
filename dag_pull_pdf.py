import logging
import re
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DAG_ID = "pull_pdf_lakehouse"
REMOTE_USER = "administrateur"
REMOTE_HOST = "srvlakehouse"
REMOTE_SCRIPT = "/opt/pull_pdf.sh"
LAKEHOUSE_PASSWORD_VARIABLE = "password_lakehouse"
LIVEDATA_PASSWORD_VARIABLE = "password_livedata"
SSH_TIMEOUT_SECONDS = 600


default_args = {
    "owner": "DATAIA",
    "depends_on_past": False,
    "retries": 0,
}


def _select_password(prompt_context: str, lakehouse_password: str, livedata_password: str) -> str:
    context = prompt_context.lower()

    if "administrateur" in context or "srvlakehouse" in context:
        return lakehouse_password

    if "adminis" in context or "srvis-tc-livedata" in context or "livedata" in context:
        return livedata_password

    raise RuntimeError(
        "Prompt de mot de passe non reconnu. "
        "Impossible de choisir entre password_lakehouse et password_livedata."
    )


def run_pull_pdf_on_lakehouse() -> None:
    import errno
    import os
    import pty
    import select
    import time

    logger = logging.getLogger(__name__)
    lakehouse_password = Variable.get(LAKEHOUSE_PASSWORD_VARIABLE)
    livedata_password = Variable.get(LIVEDATA_PASSWORD_VARIABLE)

    args = [
        "-tt",
        "-o",
        "StrictHostKeyChecking=accept-new",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        "sudo",
        "bash",
        REMOTE_SCRIPT,
    ]

    logger.info("Lancement distant : ssh -tt %s@%s sudo bash %s", REMOTE_USER, REMOTE_HOST, REMOTE_SCRIPT)

    master_fd, slave_fd = pty.openpty()
    process = subprocess.Popen(
        ["ssh", *args],
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )
    os.close(slave_fd)

    password_prompt = re.compile(r"(?:password|mot de passe).*:", re.IGNORECASE)
    host_key_prompt = re.compile(r"are you sure you want to continue connecting", re.IGNORECASE)
    output_buffer = ""
    prompt_context = ""
    last_output_at = time.monotonic()

    def send_line(value: str) -> None:
        os.write(master_fd, f"{value}\n".encode("utf-8"))

    try:
        while process.poll() is None:
            ready, _, _ = select.select([master_fd], [], [], 1)

            if not ready:
                if time.monotonic() - last_output_at > SSH_TIMEOUT_SECONDS:
                    process.kill()
                    raise TimeoutError(
                        f"Aucune sortie recue pendant {SSH_TIMEOUT_SECONDS}s pendant l'execution de {REMOTE_SCRIPT}."
                    )
                continue

            try:
                chunk = os.read(master_fd, 4096)
            except OSError as exc:
                if exc.errno == errno.EIO:
                    break
                raise

            if not chunk:
                break

            last_output_at = time.monotonic()
            text = chunk.decode("utf-8", errors="replace")
            output_buffer += text
            prompt_context = (prompt_context + text)[-4000:]

            while "\n" in output_buffer:
                line, output_buffer = output_buffer.split("\n", 1)
                if line.strip():
                    logger.info(line.rstrip())

            if host_key_prompt.search(prompt_context):
                send_line("yes")
                prompt_context = ""
                continue

            if password_prompt.search(prompt_context):
                send_line(_select_password(prompt_context, lakehouse_password, livedata_password))
                prompt_context = ""

        remaining_output = output_buffer.strip()
        if remaining_output:
            logger.info(remaining_output)
    finally:
        os.close(master_fd)

    return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(f"Echec du script distant {REMOTE_SCRIPT}, code retour {return_code}.")

    logger.info("Script distant termine avec succes.")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 12 * * 0",
    catchup=False,
    tags=["pdf", "lakehouse", "livedata"],
) as dag:

    pull_pdf = PythonOperator(
        task_id="run_pull_pdf_on_lakehouse",
        python_callable=run_pull_pdf_on_lakehouse,
        execution_timeout=timedelta(hours=8),
    )

    pull_pdf
