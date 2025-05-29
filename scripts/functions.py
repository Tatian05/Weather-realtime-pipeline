def delivery_callback(err, msg):
    if err:
        print(f"Error sending message: {err}")
    else:
        print(f"Message delivered successfully to {msg.topic()} [Partition: {msg.partition()}] with offset {msg.offset()}")