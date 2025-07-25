import logging
import json
import os
import azure.functions as func
from azure.iot.hub import IoTHubRegistryManager

# IoT Hub 연결 문자열 (환경 변수로 설정)
IOT_HUB_CONNECTION_STRING = os.environ["IOT_HUB_CONNECTION_STRING"]
# NOTE: Hardcoding connection strings is generally not recommended for production.
# Use Application Settings in Function App for secure storage.
ROBOT_DEVICE_ID = "water-purifier-robot-01"

logging.basicConfig(level=logging.INFO)

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        logging.info(f"==== Received raw data from Stream Analytics: {req_body}")

        data_list = []
        if isinstance(req_body, list):
            data_list = req_body
        elif req_body is not None: # Ensure it's not None if not a list
            data_list = [req_body]

        if not data_list: # Handle empty list or no valid data after parsing
            logging.info("Received empty or invalid data list from Stream Analytics. Returning 200 OK.")
            return func.HttpResponse(
                "No valid data in request body, but processed successfully (nothing to do).",
                status_code=200 # Return 200 if nothing to process
            )

        for data in data_list:
            command_to_send = None
            # Use .get() with a default to avoid KeyError if fields are missing
            command_type = data.get("command_type")
            sensor_id = data.get("sensor_id")
            reason_message = data.get("reason_message", "Water quality issue detected")

            if command_type == "adjust_ph_up":
                command_to_send = {"command": "adjust_ph_up", "target_ph": 7.0, "reason": reason_message, "source_sensor": sensor_id}
            elif command_type == "adjust_ph_down":
                command_to_send = {"command": "adjust_ph_down", "target_ph": 7.5, "reason": reason_message, "source_sensor": sensor_id}
            elif command_type == "activate_filter":
                command_to_send = {"command": "activate_filter", "duration_minutes": 60, "reason": reason_message, "source_sensor": sensor_id}
            else:
                logging.info(f"No specific command type detected or command type not supported for data: {data}. Skipping.")

            if command_to_send:
                logging.info(f"Sending command to robot {ROBOT_DEVICE_ID}: {command_to_send}")
                await send_c2d_message(ROBOT_DEVICE_ID, command_to_send)
            else:
                logging.info("No command generated for this event.")

        return func.HttpResponse(
             "Successfully processed Stream Analytics output.",
             status_code=200
        )

    except ValueError as ve: # Specifically catch JSON parsing errors
        logging.error(f"Invalid JSON in request body: {ve}. Request body: {req.get_body().decode('utf-8')}")
        return func.HttpResponse(
             f"Invalid JSON in request body: {ve}",
             status_code=400
        )
    except Exception as e:
        logging.error(f"Error processing request: {e}", exc_info=True) # exc_info=True for full stack trace
        return func.HttpResponse(
             f"An internal server error occurred: {e}",
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
        logging.error(f"Error sending C2D message to {device_id}: {e}", exc_info=True)
    finally:
        if registry_manager:
            registry_manager.close()