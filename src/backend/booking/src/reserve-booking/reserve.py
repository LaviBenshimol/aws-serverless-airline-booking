import datetime
import os
import uuid
import random
import boto3
from botocore.exceptions import ClientError


from lambda_python_powertools.logging import (
    logger_inject_process_booking_sfn,
    logger_setup,
    MetricUnit,
    log_metric,
)

from lambda_python_powertools.tracing import Tracer

logger = logger_setup()
tracer = Tracer()

session = boto3.Session()
dynamodb = session.resource("dynamodb")
table_name = os.getenv("BOOKING_TABLE_NAME", "undefined")
table = dynamodb.Table(table_name)
# 

def get_config(config_id, values):
    extract_values = lambda items: [value for value in [values for values in items[0].values()][0].values()][0]
    client = boto3.client('dynamodb')
    items = client.query(TableName='configuration_table', KeyConditionExpression='configID = :config',
                         ExpressionAttributeValues={
                             ':config': {'S': config_id}
                         }, ProjectionExpression=values)['Items']
    return extract_values(items)


_cold_start = True


class BookingReservationException(Exception):
    def __init__(self, message=None, status_code=None, details=None):

        super(BookingReservationException, self).__init__()

        self.message = message or "Booking reservation failed"
        self.status_code = status_code or 500
        self.details = details or {}


def is_booking_request_valid(booking):
    return all(x in booking for x in ["outboundFlightId", "customerId", "chargeId"])


@tracer.capture_method
def reserve_booking(booking,executeAnomaly):
    """Creates a new booking as UNCONFIRMED

    Parameters
    ----------
    booking: dict
        chargeId: string
            pre-authorization charge ID

        stateExecutionId: string
            Step Functions Process Booking Execution ID

        chargeId: string
            Pre-authorization payment token

        customer: string
            Customer unique identifier

        bookingOutboundFlightId: string
            Outbound flight unique identifier

    Returns
    -------
    dict
        bookingId: string
    """
    try:
        booking_id = str(uuid.uuid4())
        state_machine_execution_id = booking["name"]
        outbound_flight_id = booking["outboundFlightId"]
        customer_id = booking["customerId"]
        payment_token = booking["chargeId"]

        booking_item = {
            "id": booking_id,
            "stateExecutionId": state_machine_execution_id,
            "__typename": "Booking",
            "bookingOutboundFlightId": outbound_flight_id,
            "checkedIn": False,
            "customer": customer_id,
            "paymentToken": payment_token,
            "status": "UNCONFIRMED",
            "createdAt": str(datetime.datetime.now()),
        }

        logger.debug(
            {"operation": "reserve_booking", "details": {"outbound_flight_id": outbound_flight_id}}
        )
        num_of_oper = 1
        
        if executeAnomaly: 
            num_of_oper = 20
            print('ANOMALY! START: {}, SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format('START','Airline-ReserveBooking-master',table_name,'putObject','DenialOfWalletMany'))
        
        for i in range(num_of_oper):
            ret = table.put_item(Item=booking_item)
            
        if executeAnomaly:   
            print('ANOMALY! START: {}, SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format('START','Airline-ReserveBooking-master',table_name,'putObject','DenialOfWalletMany'))
        
        logger.info({"operation": "reserve_booking", "details": ret})
        logger.debug("Adding put item operation result as tracing metadata")
        tracer.put_metadata(booking_id, booking_item, "booking")

        return {"bookingId": booking_id}
    except ClientError as err:
        logger.debug({"operation": "reserve_booking", "details": err})
        raise BookingReservationException(details=err)


@tracer.capture_lambda_handler(process_booking_sfn=True)
@logger_inject_process_booking_sfn
def lambda_handler(event, context):
    """AWS Lambda Function entrypoint to reserve a booking

    Parameters
    ----------
    event: dict, required
        Step Functions State Machine event

        chargeId: string
            pre-authorization charge ID

        stateExecutionId: string
            Step Functions Process Booking Execution ID

        chargeId: string
            Pre-authorization payment token

        customerId: string
            Customer unique identifier

        bookingOutboundFlightId: string
            Outbound flight unique identifier

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    bookingId: string
        booking ID generated

    Raises
    ------
    BookingReservationException
        Booking Reservation Exception including error message upon failure
    """
    global _cold_start
    
    anomaly_mode = get_config('anomaly_mode', 'Activate')
    anomaly_prob = float(get_config('Airline-ReserveBooking-master', 'anomaly_prob'))
    anomaluseExecution = random.random() < anomaly_prob 
        # if both ANOMALY_MODE and anomaluseExecution are true - execute anomaly
    executeAnomaly = anomaly_mode == True & anomaluseExecution == True
    cancel_path = get_config('Airline-ReserveBooking-master', 'cancel_path')
    cacnel_prob = float(get_config('Airline-ReserveBooking-master', 'cancel_prob'))
    cancelExecution = random.random() < cancel_prob
    executeCancel = cancel_path == True & cancelExecution == True
    
    if executeCancel:
        x = create_error

    if _cold_start:
        log_metric(
            name="ColdStart", unit=MetricUnit.Count, value=1, function_name=context.function_name
        )
        _cold_start = False

    if not is_booking_request_valid(event):
        log_metric(
            name="InvalidBookingRequest",
            unit=MetricUnit.Count,
            value=1,
            operation="reserve_booking",
        )
        logger.error({"operation": "invalid_event", "details": event})
        raise ValueError("Invalid booking request")

    try:
        logger.debug(f"Reserving booking for customer {event['customerId']}")
        ret = reserve_booking(event,executeAnomaly)

        log_metric(name="SuccessfulReservation", unit=MetricUnit.Count, value=1)
        logger.debug("Adding Booking Reservation annotation")
        tracer.put_annotation("Booking", ret["bookingId"])
        tracer.put_annotation("BookingStatus", "RESERVED")

        # Step Functions use the return to append `bookingId` key into the overall output
        return ret["bookingId"]
    except BookingReservationException as err:
        log_metric(name="FailedReservation", unit=MetricUnit.Count, value=1)
        logger.debug("Adding Booking Reservation annotation before raising error")
        tracer.put_annotation("BookingStatus", "ERROR")
        logger.error({"operation": "reserve_booking", "details": err})
        raise BookingReservationException(details=err)
