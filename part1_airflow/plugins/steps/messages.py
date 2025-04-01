from airflow.providers.telegram.hooks.telegram import TelegramHook
import logging

logger = logging.getLogger(__name__)

def send_telegram_success_message(context):
    hook = TelegramHook(token='7376174360:AAEsGCaKFKxJ0iVr1J9aNaInw5RCsL6H6PI', chat_id='-1002275348874')
    dag = context['dag'].dag_id
    run_id = context['run_id']
        
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': '-1002275348874',
        'text': message
    })

def send_telegram_failure_message(context):
    hook = TelegramHook(token='7376174360:AAEsGCaKFKxJ0iVr1J9aNaInw5RCsL6H6PI', chat_id='-1002275348874')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
        
    message = f'Исполнение DAG {dag} с id={run_id} выдало ошибку {task_instance_key_str}!'
    hook.send_message({
        'chat_id': '-1002275348874',
        'text': message
    })