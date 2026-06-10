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
LIVEDATA_HOST_ALIASES = ("srvis-tc-livedata", "livedata", "10.220.4.105")


default_args = {
    "owner": "DATAIA",
    "depends_on_past": False,
    "retries": 0,
}


def _select_password(
    prompt_context: str,
    lakehouse_password: str,
    livedata_password: str,
    password_prompt_count: int,
) -> tuple[str, str]:
    prompt_line = _extract_password_prompt(prompt_context).lower()
    context = prompt_line or prompt_context.lower()[-1000:]

    password_markers = [context.rfind("password"), context.rfind("mot de passe")]
    last_marker = max(password_markers)
    if last_marker >= 0:
        context = context[max(0, last_marker - 300):]

    if "adminis" in context or any(alias in context for alias in LIVEDATA_HOST_ALIASES):
        return livedata_password, LIVEDATA_PASSWORD_VARIABLE

    if "administrateur" in context or "srvlakehouse" in context:
        return lakehouse_password, LAKEHOUSE_PASSWORD_VARIABLE

    if password_prompt_count <= 2:
        return lakehouse_password, LAKEHOUSE_PASSWORD_VARIABLE

    return livedata_password, LIVEDATA_PASSWORD_VARIABLE


def _extract_password_prompt(prompt_context: str) -> str:
    matches = re.findall(r"[^\r\n]*(?:password|mot de passe)[^:\r\n]*:", prompt_context, re.IGNORECASE)
    return matches[-1].strip() if matches else ""


def _mask_sensitive(value: str, *secrets: str) -> str:
    masked = value
    for secret in secrets:
        if secret:
            masked = masked.replace(secret, "***")
    return masked


def _format_debug_output(value: str, *secrets: str) -> str:
    visible = _mask_sensitive(value, *secrets)
    return repr(visible[-1000:])


def run_pull_pdf_on_lakehouse() -> None:
    import errno
    import os
    import pty
    import select
    import time

    logger = logging.getLogger(__name__)
    lakehouse_password = Variable.get(LAKEHOUSE_PASSWORD_VARIABLE).strip()
    livedata_password = Variable.get(LIVEDATA_PASSWORD_VARIABLE).strip()

    args = [
        "-tt",
        "-o",
        "BatchMode=no",
        "-o",
        "PreferredAuthentications=password",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "NumberOfPasswordPrompts=10",
        "-o",
        "StrictHostKeyChecking=no",
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

    password_prompt = re.compile(r"(?:password|mot de passe)", re.IGNORECASE)
    host_key_prompt = re.compile(r"are you sure you want to continue connecting", re.IGNORECASE)
    output_buffer = ""
    prompt_context = ""
    recent_output = ""
    password_prompt_count = 0
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
            recent_output = (recent_output + text)[-4000:]

            if (
                password_prompt.search(text)
                or "Permission denied" in text
                or "disconnect" in text.lower()
                or ("\n" not in text and text.strip())
            ):
                logger.info(
                    "DEBUG sortie SSH brute masquee: %s",
                    _format_debug_output(text, lakehouse_password, livedata_password),
                )
                logger.info(
                    "DEBUG contexte prompt masque: %s",
                    _format_debug_output(prompt_context, lakehouse_password, livedata_password),
                )

            while "\n" in output_buffer:
                line, output_buffer = output_buffer.split("\n", 1)
                if line.strip():
                    logger.info(_mask_sensitive(line.rstrip(), lakehouse_password, livedata_password))

            if host_key_prompt.search(prompt_context):
                send_line("yes")
                prompt_context = ""
                continue

            if password_prompt.search(prompt_context):
                password_prompt_count += 1
                prompt_line = _extract_password_prompt(prompt_context)
                selected_password, password_source = _select_password(
                    prompt_context,
                    lakehouse_password,
                    livedata_password,
                    password_prompt_count,
                )
                logger.info(
                    "Prompt mot de passe #%s detecte (%s), reponse avec la variable Airflow %s.",
                    password_prompt_count,
                    prompt_line or "prompt non isole",
                    password_source,
                )
                send_line(selected_password)
                prompt_context = ""

        remaining_output = output_buffer.strip()
        if remaining_output:
            logger.info(_mask_sensitive(remaining_output, lakehouse_password, livedata_password))
    finally:
        os.close(master_fd)

    return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(
            f"Echec du script distant {REMOTE_SCRIPT}, code retour {return_code}. "
            f"Derniere sortie recue: {_mask_sensitive(recent_output.strip()[-1000:], lakehouse_password, livedata_password)}"
        )

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
