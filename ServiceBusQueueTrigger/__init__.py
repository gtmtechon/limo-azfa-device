import logging
import json
import os
import azure.functions as func
import sendgrid
from sendgrid.helpers.mail import Email, Mail, Personalization

logger = logging.getLogger(__name__)
app = func.FunctionApp() # 기존 app 변수가 있다면 제거하고 이 함수에만 사용

@app.service_bus_queue_trigger(arg_name="msg",
                               queue_name="robot-battery-alert-queue",
                               connection="AzureWebJobsServiceBus") # local.settings.json에 설정된 이름
def SendBatteryAlertEmail(msg: func.ServiceBusMessage):
    logger.info('Python ServiceBus queue trigger processed message.')

    try:
        message_body = msg.get_body().decode('utf-8')
        logger.info(f"Received message body: {message_body}")

        data = json.loads(message_body)
        device_id = data.get('deviceId', 'N/A')
        battery_level = data.get('batteryLevel', 'N/A')
        timestamp = data.get('ttimestamp', 'N/A') # ttimestamp 사용

        alert_subject = f"[긴급] 로봇 배터리 부족 알림 - {device_id}"
        alert_content = (
            f"로봇 ID: {device_id} (시간: {timestamp})\n"
            f"배터리 잔량이 {battery_level}% 입니다. 즉시 충전이 필요합니다.\n\n"
            "호수 수질 관리 시스템에서 확인해주세요."
        )

        logger.warning(f"Sending email alert: {alert_subject} - {alert_content}")

        # SendGrid를 이용한 이메일 발송
        sg = sendgrid.SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        mail = Mail()
        mail.from_email = Email(os.environ.get('SENDER_EMAIL'))
        personalization = Personalization()
        personalization.add_to(Email(os.environ.get('RECIPIENT_EMAIL')))
        mail.add_personalization(personalization)
        mail.subject = alert_subject
        mail.add_content("text/plain", alert_content)

        response = sg.send(mail)
        logger.info(f"Email sent. Status Code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error processing message or sending email: {e}")