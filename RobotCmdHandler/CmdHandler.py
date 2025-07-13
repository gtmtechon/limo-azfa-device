import logging
import json
import os
import azure.functions as func
from azure.iot.hub import IoTHubRegistryManager

# IoT Hub 연결 문자열 (환경 변수로 설정)
#IOT_HUB_CONNECTION_STRING = os.environ["IoTHubConnectionString"]
IOT_HUB_CONNECTION_STRING = "HostName=iothub-iot01.azure-devices.net;DeviceId=water-purifier-robot-01;SharedAccessKey=p5YkARBM6DYIzASe+Mzk95EiYFRjEzeQ63E9hbol/TI="

ROBOT_DEVICE_ID = "water-purifier-robot-01" # IoT Hub에 등록된 로봇의 Device ID

logging.basicConfig(level=logging.INFO)

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Stream Analytics에서 보낸 JSON 데이터 읽기
        req_body = req.get_json()
        logging.info(f"Received data from Stream Analytics: {req_body}")

        # Stream Analytics가 배열로 보낼 수 있으므로 리스트 처리
        # ASA의 출력 형식에 따라 단일 객체 또는 객체 배열일 수 있음.
        # ASA에서 "JSON 배열로 직렬화" 옵션을 켜면 배열로 보냄.
        if isinstance(req_body, list):
            data_list = req_body
        else:
            data_list = [req_body]

        for data in data_list:
            command_to_send = None
            command_type = data.get("command_type") # ASA 쿼리에서 생성한 명령 타입
            sensor_id = data.get("sensor_id")
            reason_message = data.get("reason_message", "Water quality issue detected")

            if command_type == "adjust_ph_up":
                command_to_send = {"command": "adjust_ph_up", "target_ph": 7.0, "reason": reason_message, "source_sensor": sensor_id}
            elif command_type == "adjust_ph_down":
                command_to_send = {"command": "adjust_ph_down", "target_ph": 7.5, "reason": reason_message, "source_sensor": sensor_id}
            elif command_type == "activate_filter":
                command_to_send = {"command": "activate_filter", "duration_minutes": 60, "reason": reason_message, "source_sensor": sensor_id}
            else:
                logging.info(f"No specific command type detected or command type not supported: {command_type}")

            if command_to_send:
                logging.info(f"Sending command to robot {ROBOT_DEVICE_ID}: {command_to_send}")
                await send_c2d_message(ROBOT_DEVICE_ID, command_to_send)
            else:
                logging.info("No command generated for this event.")

        return func.HttpResponse(
             "Successfully processed Stream Analytics output.",
             status_code=200
        )

    except ValueError:
        return func.HttpResponse(
             "Invalid JSON in request body.",
             status_code=400
        )
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(
             f"An error occurred: {e}",
             status_code=500
        )

async def send_c2d_message(device_id, message_payload):
    registry_manager = None
    try:
        registry_manager = IoTHubRegistryManager.from_connection_string(IOT_HUB_CONNECTION_STRING)
        message = json.dumps(message_payload)
        await registry_manager.send_c2d_message(device_id, message)
        logging.info(f"C2D message sent successfully to device {device_id}.")
    except Exception as e:
        logging.error(f"Error sending C2D message to {device_id}: {e}")
    finally:
        if registry_manager:
            registry_manager.close()