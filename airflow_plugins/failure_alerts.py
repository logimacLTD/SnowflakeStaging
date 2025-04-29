from airflow.utils.email import send_email

def failure_callback(context):
    subject = f"Airflow Task Failed: {context['task_instance'].task_id}"
    html_content = f"""
    DAG: {context['task_instance'].dag_id}<br>
    Task: {context['task_instance'].task_id}<br>
    Execution Time: {context['execution_date']}<br>
    Log Url: {context['task_instance'].log_url}<br>
    """
    send_email(
        to=["your-team@email.com"],
        subject=subject,
        html_content=html_content
    )