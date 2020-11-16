# Import the libraries
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import translate_v2 as translate
from google.cloud import vision
import base64
import json
import os

# Instantiates a clients
vision_client = vision.ImageAnnotatorClient()
translate_client = translate.Client()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

def send_extracted(text, filename, src_lang):
    """Send extracted text to translating or result queue"""

    project_id = os.environ["PROJECT_ID"]
    topic_name = os.environ["TRANSLATE_TOPIC"]
    target_langs = os.environ["TARGET_LANG"].split(",")
    
    # If source language matches with target language or undefined
    # Skip translating and send to RESULT_TOPIC
    if (len(target_langs) == 1 and src_lang == target_langs) or src_lang == "und":
        topic_name = os.environ["RESULT_TOPIC"]
    
    # Message body
    message = {
        "text": text,
        "filename": filename,
        "lang": target_langs,
        "src_lang": src_lang,
    }

    # Topic to publish messages
    topic_path = publisher.topic_path(project_id, topic_name)
    # Create a bytestring representing the message body
    message_data = json.dumps(message).encode("utf-8")
    # Publishing messages to Pub/Sub
    future = publisher.publish(topic_path, data=message_data)
    future.result()

def send_translated(text, filename, target_lang):
    """Send extracted text to result queue"""

    project_id = os.environ["PROJECT_ID"]
    topic_name = os.environ["RESULT_TOPIC"]

    # Message body
    message = {
        "text": text,
        "filename": filename
    }

    # Topic to publish messages
    topic_path = publisher.topic_path(project_id, topic_name)
    # Create a bytestring representing the message body
    message_data = json.dumps(message).encode("utf-8")
    # Publishing messages to Pub/Sub
    future = publisher.publish(topic_path, data=message_data)
    future.result()

def validate_property(event, property):
    """Validating if property exist in request then return its value"""
    if property not in event:
        raise ValueError(
            "{} is not provided. Make sure you have property {} in the request"
            .format(property, property)
        )

    return event[property]

def extract_text(event, context):
    """Background Cloud Function to be triggered by Cloud Storage 
       logs relevant data when a file is changed/uploaded
    """
    # Validate and get property value
    bucket = validate_property(event, "bucket")
    filename = validate_property(event, "name")

    # Loads the image from GCS
    image = vision.Image(
        source=vision.ImageSource(gcs_image_uri=f"gs://{bucket}/{filename}")
    )

    # Perform text detection
    print("Extracting text from {}".format(f"gs://{bucket}/{filename}"))
    response = vision_client.text_detection(image=image)
    result = response.text_annotations
    if len(result) > 0:
        text = result[0].description.lstrip()
        print("Detected text: {}".format(repr(text)))
    else:
        print("No text detected from {}".format(filename))
        return None # end
    
    # Perform language detection
    detect_lang = translate_client.detect_language(text)
    src_lang = detect_lang["language"]
    print("Detected source language: '{}'".format(src_lang))

    # Send to Pub/Sub
    print("Sending message to Pub/Sub")
    send_extracted(text, filename, src_lang)
    print("Finished extracting file {}".format(filename))

def translate_text(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Translating text to target languages"""

    # The Pub/Sub message's data is stored as a base64-encoded string in the data property
    if 'data' in event:
        # Decode to extract data
        data = base64.b64decode(event["data"]).decode("utf-8")
        message = json.loads(data)
    else:
        raise ValueError("Data sector is missing in the Pub/Sub message.")

    # Validate and get property value
    text = validate_property(message, "text")
    filename = validate_property(message, "filename")
    src_lang = validate_property(message, "src_lang")
    target_lang = validate_property(message, "lang")

    print("Received request to translating file {}".format(filename))

    # Perform Translation
    print("Translating text into: {}".format(target_lang))
    translated_text = []
    for lang in target_lang:
        # Skip translating if source language matches with target language translating
        if lang == src_lang:
            result = text.replace("\n", " ")
        else:
            response = translate_client.translate(
                text, target_language=lang, source_language=src_lang)
            result = response['translatedText']
        translated_text.append(f"{lang}: {result}")
    text = "\n".join(translated_text)+"\n"

    # Send to Pub/Sub
    print("Sending message to Pub/Sub")
    send_translated(text, filename, target_lang)
    print("Finished translating file {}".format(filename))

def save_result(event, context):    
    """Background Cloud Function to be triggered by Pub/Sub.
    Send result text to result
    """
    # The Pub/Sub message's data is stored as a base64-encoded string in the data property
    if 'data' in event:
        # Decode to extract data
        message_data = base64.b64decode(event["data"]).decode("utf-8")
        message = json.loads(message_data)
    else:
        raise ValueError("Data sector is missing in the Pub/Sub message.")

    # Validate and get property value
    text = validate_property(message, "text")
    filename = validate_property(message, "filename")

    print("Received request to save file {}".format(filename))

    bucket_name = os.environ["RESULT_BUCKET"]
    result_filename = "{}_translated.txt".format(filename)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(result_filename)

    # Upload blob's content
    blob.upload_from_string(text)
    print("File saved to {}".format(f"gs://{bucket}/{filename}"))