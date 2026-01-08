
from airflow.models import DagRun, DagModel
from airflow.utils.session import provide_session
from airflow.utils.state import State
from datetime import datetime


@provide_session
def build_dag_status_table(session=None):
    rows = []

    dags = session.query(DagModel).filter(DagModel.is_active == True).all()

    for dag in dags:
        last_run = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id)
            .order_by(DagRun.execution_date.desc())
            .first()
        )

        status = last_run.state if last_run else "no_run"

        color = {
            State.SUCCESS: "green",
            State.FAILED: "red",
            State.RUNNING: "orange",
        }.get(status, "gray")

        rows.append(
            f"""
            <tr>
                <td>{dag.dag_id}</td>
                <td style="color:{color}; font-weight:bold">{status}</td>
            </tr>
            """
        )

    html = f"""
    <h3>Airflow DAGs – Dernier statut</h3>
    <table border="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>DAG</th>
            <th>Status</th>
        </tr>
        {''.join(rows)}
    </table>
    """

    return html

