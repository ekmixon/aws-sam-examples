from __future__ import print_function
import json


def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    notification_type = message['notificationType']
    handlers.get(notification_type, handle_unknown_type)(message)


def handle_bounce(message):
    message_id = message['mail']['messageId']
    bounced_recipients = message['bounce']['bouncedRecipients']
    addresses = [recipient['emailAddress'] for recipient in bounced_recipients]
    bounce_type = message['bounce']['bounceType']
    print(
        f'Message {message_id} bounced when sending to {", ".join(addresses)}. Bounce type: {bounce_type}'
    )


def handle_complaint(message):
    message_id = message['mail']['messageId']
    complained_recipients = message['complaint']['complainedRecipients']
    addresses = [recipient['emailAddress'] for recipient in complained_recipients]
    print(
        f'A complaint was reported by {", ".join(addresses)} for message {message_id}.'
    )


def handle_delivery(message):
    message_id = message['mail']['messageId']
    delivery_timestamp = message['delivery']['timestamp']
    print(
        f"Message {message_id} was delivered successfully at {delivery_timestamp}"
    )


def handle_unknown_type(message):
    print("Unknown message type:\n%s" % json.dumps(message))
    raise Exception(
        f"Invalid message type received: {message['notificationType']}"
    )


handlers = {"Bounce": handle_bounce,
            "Complaint": handle_complaint,
            "Delivery": handle_delivery}
