import logging
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


class _LogWriter:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self._buffer = ""

    def write(self, data: str) -> None:
        self._buffer += data
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self.logger.info(line.rstrip())

    def flush(self) -> None:
        if self._buffer.strip():
            self.logger.info(self._buffer.rstrip())
        self._buffer = ""


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
    try:
        import pexpect
    except ImportError as exc:
        raise RuntimeError(
            "Le package pexpect est requis pour piloter les prompts SSH/sudo. "
            "Ajoutez pexpect aux dependances Airflow."
        ) from exc

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

    child = pexpect.spawn(
        "ssh",
        args,
        encoding="utf-8",
        codec_errors="replace",
        timeout=SSH_TIMEOUT_SECONDS,
    )
    child.logfile_read = _LogWriter(logger)

    patterns = [
        pexpect.EOF,
        pexpect.TIMEOUT,
        r"(?i)are you sure you want to continue connecting",
        r"(?i)(?:password|mot de passe).*:",
    ]

    try:
        while True:
            matched = child.expect(patterns)

            if matched == 0:
                break

            if matched == 1:
                raise TimeoutError(
                    f"Aucune sortie recue pendant {SSH_TIMEOUT_SECONDS}s pendant l'execution de {REMOTE_SCRIPT}."
                )

            if matched == 2:
                child.sendline("yes")
                continue

            prompt_context = f"{child.before}{child.after}"
            child.sendline(_select_password(prompt_context, lakehouse_password, livedata_password))
    finally:
        if child.logfile_read:
            child.logfile_read.flush()
        child.close()

    if child.exitstatus != 0:
        raise RuntimeError(f"Echec du script distant {REMOTE_SCRIPT}, code retour {child.exitstatus}.")

    if child.signalstatus is not None:
        raise RuntimeError(f"Le script distant {REMOTE_SCRIPT} a ete interrompu par le signal {child.signalstatus}.")

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
