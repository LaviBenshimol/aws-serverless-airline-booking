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
def reserve_booking(booking,dow_executeAnomaly,update_item_executeAnomaly,rid):
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
        
        if update_item_executeAnomaly:
            num_of_oper = 0
            print('ANOMALY! REQUEST_ID: {}, START: {}, SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format(rid,'START','Airline-ReserveBooking-master',table_name,'updateItem','UpdateItemInsteadOfPutItem'))
            ret = table.update_item(Key={'id':'bf313090-82f4-4698-8eb8-29489f242c7d'},
                                UpdateExpression="set checkedIn=:r",
                                ExpressionAttributeValues={
                                    ':r': True
                                },
                                ReturnValues="UPDATED_NEW")
        if dow_executeAnomaly: 
            num_of_oper = 20
            print('ANOMALY! REQUEST_ID: {}, START: {}, SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format(rid,'START','Airline-ReserveBooking-master',table_name,'putObject','DenialOfWalletMany'))
        
        for i in range(num_of_oper):
            ret = table.put_item(Item=booking_item)
            
        if dow_executeAnomaly:   
            print('ANOMALY! REQUEST_ID: {}, START: {}, SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format(rid,'START','Airline-ReserveBooking-master',table_name,'putObject','DenialOfWalletMany'))
        
        
        
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
    print("Reserve")
    anomaly_mode = get_config('anomaly_mode', 'Activate')
    dow_anomaly_prob = float(get_config('Airline-ReserveBooking-master-DOW', 'anomaly_prob'))
    dow_anomaluseExecution = random.random() < dow_anomaly_prob 
        # if both ANOMALY_MODE and anomaluseExecution are true - execute anomaly
    dow_executeAnomaly = anomaly_mode == True & dow_anomaluseExecution == True
    
    update_item_anomaly_prob = float(get_config('Airline-ReserveBooking-master-UpdateItem', 'anomaly_prob'))
    update_item_anomaluseExecution = random.random() < update_item_anomaly_prob 
        # if both ANOMALY_MODE and anomaluseExecution are true - execute anomaly
    update_item_executeAnomaly = anomaly_mode == True & update_item_anomaluseExecution == True
    
    # If two attacks were chosen  cancel one of them randomly:
    if update_item_executeAnomaly and dow_executeAnomaly:
        if random.random() < 0.5:
            update_item_executeAnomaly = False
        else:
            dow_executeAnomaly = False
              
    cancel_path = get_config('cancel_mode', 'Activate')
    cancel_prob = float(get_config('cancel_mode', 'Prob'))
    cancelExecution = random.random() < cancel_prob
    executeCancel = cancel_path == True & cancelExecution == True
    
    if executeCancel:
        raise ValueError("Cancel booking request")

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
        rid = context.aws_request_id
        ret = reserve_booking(event,dow_executeAnomaly,update_item_executeAnomaly,rid)

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
