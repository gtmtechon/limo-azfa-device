
import logging
import json
import os
import azure.functions as func

# SendGrid 라이브러리 임포트 (메일 발송 함수에 필요)
import sendgrid
from sendgrid.helpers.mail import Email, Mail, Personalization

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# !!! 중요: func.FunctionApp() 인스턴스는 프로젝트 전체에서 단 한 번만 정의되어야 합니다. !!!
# 이 파일의 최상단에 위치하는 것이 일반적입니다.
app = func.FunctionApp()

# -----------------------------------------------------------
# 기존 CmdHandler 함수 (HTTP Trigger 예시)
# -----------------------------------------------------------
# function.json 내용과 매핑되는 데코레이터 정의
# @app.http_trigger(name="req", auth_level=func.AuthLevel.FUNCTION)
# async def CmdHandler(req: func.HttpRequest) -> func.HttpResponse:
#     logger.info('Python HTTP trigger function processed a request.')
#     # ... CmdHandler의 기존 로직 ...
#     try:
#         req_body = req.get_json()
#         logger.info(f"==== Received raw data from Stream Analytics: {req_body}")
#         # ... (생략) ...
#         return func.HttpResponse("Successfully processed Stream Analytics output.", status_code=200)
#     except Exception as e:
#         logger.error(f"Error processing request in CmdHandler: {e}", exc_info=True)
#         return func.HttpResponse(f"An internal server error occurred: {e}", status_code=500)

# async def send_c2d_message(device_id, message_payload):
#     # CmdHandler에 필요한 send_c2d_message 함수
#     pass # 실제 구현은 CmdHandler와 함께 복사되어야 합니다.


# -----------------------------------------------------------
# SendBatteryAlertEmail 함수 (Service Bus Queue Trigger)
# -----------------------------------------------------------
# Service Bus Queue Trigger 바인딩 정의 (function.json 내용 대신 데코레이터 사용)
@app.service_bus_queue_trigger(arg_name="msg",
                               queue_name="robot-battery-alert-queue",
                               connection="ServiceBusConnectionString") # local.settings.json 또는 App Service 설정에 정의된 연결 문자열 이름
def SendBatteryAlertEmail(msg: func.ServiceBusMessage):
    logger.info('Python ServiceBus queue trigger processed message.')
    
    try:
        message_body = msg.get_body().decode('utf-8')
        logger.info(f"Received message body: {message_body}")
        
        # JSON 메시지 파싱
        data = json.loads(message_body)
        
        device_id = data.get('deviceId', 'N/A')
        battery_level = data.get('batteryLevel', 'N/A')
        timestamp = data.get('ttimestamp', 'N/A') # 로봇 시뮬레이터에서 ttimestamp 사용

        alert_subject = f"[긴급] 로봇 배터리 부족 알림 - {device_id}"
        alert_content = (
            f"로봇 ID: {device_id} (시간: {timestamp})\n"
            f"배터리 잔량이 {battery_level}% 입니다. 즉시 충전이 필요합니다.\n\n"
            "호수 수질 관리 시스템에서 확인해주세요."
        )
        
        logger.warning(f"Sending email alert: {alert_subject} - {alert_content}")
        
        # SendGrid를 이용한 이메일 발송
        # SENDGRID_API_KEY, SENDER_EMAIL, RECIPIENT_EMAIL 환경 변수를 Azure Function App 설정에 추가해야 함
        # try:
            # sg = sendgrid.SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
            
            # mail = Mail()
            # mail.from_email = Email(os.environ.get('SENDER_EMAIL', 'sender@example.com'))
            
            # personalization = Personalization()
            # personalization.add_to(Email(os.environ.get('RECIPIENT_EMAIL', 'recipient@example.com')))
            # mail.add_personalization(personalization)
            
            # mail.subject = alert_subject
            # mail.add_content("text/plain", alert_content)
            
            # response = sg.send(mail)
            # logger.info(f"Email sent. Status Code: {response.status_code}")
            # logger.info(f"Email Response Headers: {response.headers}")
        # except Exception as e:
        #     logger.error(f"Error sending email via SendGrid: {e}")
            
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {e}. Message body: {message_body}")
    except Exception as e:
        logger.error(f"Error processing Service Bus message: {e}", exc_info=True)