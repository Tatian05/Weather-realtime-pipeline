def delivery_callback(err, msg):
    if err:
        print(f"Error sending message: {err}")
    else:
        print(f"Message delivered successfully to {msg.topic()} [Partition: {msg.partition()}] with offset {msg.offset()}")


def print_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    # Aquí podrías agregar más lógica para inspeccionar los datos si lo deseas
    # Ejemplo: mostrar las primeras filas de cada batch
    batch_df.show()