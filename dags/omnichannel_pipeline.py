from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests

# Điền chìa khóa Telegram của bạn vào đây
BOT_TOKEN = 'YOUR_BOT_TOKEN_HERE'
CHAT_ID = 'YOUR_CHAT_ID_HERE'

# Hàm 1: Báo lỗi kèm Link xem Log
def send_telegram_failure_notification(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    # Lấy đường link log của Airflow (thay cổng 8080 mặc định thành 8081 của bạn)
    log_url = context['task_instance'].log_url.replace('8080', '8081')
    
    msg = f"❌ CẢNH BÁO!\n- Phân xưởng: {task_id}\n- Tình trạng: Sập ngầm\n- Mở hộp đen xem ngay: {log_url}"
    
    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data={'chat_id': CHAT_ID, 'text': msg})

# Hàm 2: Báo thành công (Nghiệm thu)
def send_telegram_success_notification(context):
    dag_id = context['dag'].dag_id
    
    msg = f"✅ CHÚC MỪNG!\n- Hệ thống: {dag_id}\n- Tình trạng: Đã đẩy dữ liệu lên BigQuery thành công an toàn."
    
    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data={'chat_id': CHAT_ID, 'text': msg})


default_args = {
    'owner': 'Ronaldong',
    'start_date': datetime(2026, 4, 1),
    'on_failure_callback': send_telegram_failure_notification, 
}

with DAG(
    'omnichannel_dbt_workflow',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    on_success_callback=send_telegram_success_notification # Báo thành công khi TOÀN BỘ DAG chạy xong
) as dag:

    check_conn = BashOperator(
        task_id='dbt_debug',
        bash_command='export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/marketing_project && dbt debug --profiles-dir .',
    )

    run_stg = BashOperator(
        task_id='dbt_run_staging',
        # Bạn có thể sửa dbttt thành dbt để nó chạy Success, hoặc để nguyên để test báo lỗi
        bash_command='export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/marketing_project && dbt clean && dbt run --select staging --profiles-dir .',
    )

    run_marts = BashOperator(
        task_id='dbt_build_marts',
        bash_command='export PATH=$PATH:/home/airflow/.local/bin && cd /opt/airflow/marketing_project && dbt build --select marts --profiles-dir .',
    )

    check_conn >> run_stg >> run_marts