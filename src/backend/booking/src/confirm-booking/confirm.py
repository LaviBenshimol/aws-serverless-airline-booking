import os
import secrets
import random
import boto3
from botocore.exceptions import ClientError

from lambda_python_powertools.logging import (
    MetricUnit,
    log_metric,
    logger_inject_process_booking_sfn,
    logger_setup,
)
from lambda_python_powertools.tracing import Tracer

logger = logger_setup()
tracer = Tracer()
session = boto3.Session()
dynamodb = session.resource("dynamodb")
table_name = os.getenv("BOOKING_TABLE_NAME", "undefined")
table = dynamodb.Table(table_name)

_cold_start = True

def upload_file_to_bucket(file_name):
    txt_data = b'This is the content of the file uploaded from python boto3 asdfasdf'
    s3_resource = boto3.resource('s3')
    try:
            s3_resource.Object('amplify-public-bucket', f'{file_name}.csv').put(Body=txt_data)
    except ClientError as e:
        print(e)
        return False
    return True


class BookingConfirmationException(Exception):
    def __init__(self, message=None, status_code=None, details=None):

        super(BookingConfirmationException, self).__init__()

        self.message = message or "Booking confirmation failed"
        self.status_code = status_code or 500
        self.details = details or {}
        
def get_config(config_id, values):
    extract_values = lambda items: [value for value in [values for values in items[0].values()][0].values()][0]
    client = boto3.client('dynamodb')
    items = client.query(TableName='configuration_table', KeyConditionExpression='configID = :config',
                         ExpressionAttributeValues={
                             ':config': {'S': config_id}
                         }, ProjectionExpression=values)['Items']
    return extract_values(items)


@tracer.capture_method
def confirm_booking(booking_id,DLexecuteAnomaly,PMexecuteAnomaly):
    """Update existing booking to CONFIRMED and generates a Booking reference

    Parameters
    ----------
    booking_id : string
        Unique Booking ID

    Returns
    -------
    dict
        bookingReference: string

    Raises
    ------
    BookingConfirmationException
        Booking Confirmation Exception including error message upon failure
    """
    try:
        logger.debug({"operation": "confirm_booking", "details": {"booking_id": booking_id}})
        reference = secrets.token_urlsafe(4)
        ret = table.update_item(
            Key={"id": booking_id},
            ConditionExpression="id = :idVal",
            UpdateExpression="SET bookingReference = :br, #STATUS = :confirmed",
            ExpressionAttributeNames={"#STATUS": "status"},
            ExpressionAttributeValues={
                ":br": reference,
                ":idVal": booking_id,
                ":confirmed": "CONFIRMED",
            },
            ReturnValues="UPDATED_NEW",
        )
        if DLexecuteAnomaly:
            print('ANOMALY! SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format('Airline-ConfirmBooking-master',
                                                                                'amplify-public-bucket',
                                                                                'putObject', 'DataLeakage'))
            upload_file_to_bucket(f'confirm_leak_{booking_id}');
            
        if PMexecuteAnomaly:
            print('ANOMALY! SOURCE: {}, TARGET: {}, OPERATION: {}, ANOMALY_TYPE: {}'.format('Airline-ConfirmBooking-master',
                                                                                'configuration_table',
                                                                                'Query', 'Misuse'))
            get_config('anomaly_mode', 'Activate')

        logger.info({"operation": "confirm_booking", "details": ret})
        logger.debug("Adding update item operation result as tracing metadata")
        tracer.put_metadata(booking_id, ret)

        return {"bookingReference": reference}
    except ClientError as err:
        logger.debug({"operation": "confirm_booking", "details": err})
        raise BookingConfirmationException(details=err)


@tracer.capture_lambda_handler(process_booking_sfn=True)
@logger_inject_process_booking_sfn
def lambda_handler(event, context):
    """AWS Lambda Function entrypoint to confirm booking

    Parameters
    ----------
    event: dict, required
        Step Functions State Machine event

        bookingId: string
            Unique Booking ID of an unconfirmed booking

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    string
        bookingReference generated

    Raises
    ------
    BookingConfirmationException
        Booking Confirmation Exception including error message upon failure
    """
    anomaly_mode = get_config('anomaly_mode', 'Activate')
    anomaly_prob = float(get_config('Airline-ConfirmBooking-master', 'anomaly_prob'))
    anomaluseExecution = random.random() < anomaly_prob
    DLanomaluseExecution = False
    PManomaluseExecution = False
    if anomaluseExecution:
        randNum = random.random()
        DLanomaluseExecution =  randNum < 0.5
        PManomaluseExecution = randNum  > 0.5
    # if both ANOMALY_MODE and anomaluseExecution are true - execute anomaly
    
    DLexecuteAnomaly = anomaly_mode == True & DLanomaluseExecution == True
    PMexecuteAnomaly = anomaly_mode == True & PManomaluseExecution == True
    
    cancel_path = get_config('cancel_mode', 'Activate')
    cancel_prob = float(get_config('cancel_mode', 'Prob'))
    cancelExecution = random.random() < cancel_prob
    executeCancel = cancel_path == True & cancelExecution == True
    
    if executeCancel:
        raise ValueError("Cancel booking request")
        
    global _cold_start
    if _cold_start:
        log_metric(
            name="ColdStart", unit=MetricUnit.Count, value=1, function_name=context.function_name
        )
        _cold_start = False

    booking_id = event.get("bookingId")
    if not booking_id:
        log_metric(
            name="InvalidBookingRequest",
            unit=MetricUnit.Count,
            value=1,
            operation="confirm_booking",
        )
        logger.error({"operation": "invalid_event", "details": event})
        raise ValueError("Invalid booking ID")

    try:
        logger.debug(f"Confirming booking - {booking_id}")
        ret = confirm_booking(booking_id,DLexecuteAnomaly,PMexecuteAnomaly)

        log_metric(name="SuccessfulBooking", unit=MetricUnit.Count, value=1)
        logger.debug("Adding Booking Status annotation")
        tracer.put_annotation("BookingReference", ret["bookingReference"])
        tracer.put_annotation("BookingStatus", "CONFIRMED")

        # Step Functions use the return to append `bookingReference` key into the overall output
        return ret["bookingReference"]
    except BookingConfirmationException as err:
        log_metric(name="FailedBooking", unit=MetricUnit.Count, value=1)
        logger.debug("Adding Booking Status annotation before raising error")
        tracer.put_annotation("BookingStatus", "ERROR")
        logger.error({"operation": "confirm_booking", "details": err})

        raise BookingConfirmationException(details=err)
