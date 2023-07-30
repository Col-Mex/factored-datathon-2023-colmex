import asyncio
from azure.eventhub.aio import EventHubConsumerClient

EVENT_HUB_NAME = "factored_datathon_amazon_reviews_1"
POLICY_NAME = "datathon_group_1"
EVENT_HUB_CONNECTION_STR = f"Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName={POLICY_NAME};SharedAccessKey=2GETvVt0FxyM0bo0Qau4inlmC/w3t4Uut+AEhAnAEgk=;EntityPath={EVENT_HUB_NAME}"
CONSUMER_GROUP = "colmex"

async def on_event(partition_context, event):
    print('Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id))
    
    await partition_context.update_checkpoint(event)

async def receive():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENT_HUB_NAME)
    
    async with client:
        await client.receive(
            on_event=on_event, 
            starting_position="-1")

if __name__ == "__main__":
    asyncio.run(receive())
