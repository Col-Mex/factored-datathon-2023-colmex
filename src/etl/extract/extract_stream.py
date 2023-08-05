import asyncio
import json
from azure.eventhub.aio import EventHubConsumerClient

config_file = 'jsons/config.json'

try:
    with open(config_file) as f:
        config = json.load(f)
except FileNotFoundError:
    msg = f'Ensure {config_file} exists'
    print(msg)
    exit()

EVENT_HUB_NAME = config['event_hub_name']
POLICY_NAME = config['policy_name']
CONSUMER_GROUP = config['consumer_group']

EVENT_HUB_CONNECTION_STR = f"Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName={POLICY_NAME};SharedAccessKey=2GETvVt0FxyM0bo0Qau4inlmC/w3t4Uut+AEhAnAEgk=;EntityPath={EVENT_HUB_NAME}"

STARTING_POSITION = config['starting_position']

receive = True
samples = []

client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENT_HUB_NAME)

async def on_event(partition_context, event):
    if(event != None):
        sample_str = event.body_as_str(encoding="UTF-8")
        sample = json.loads(sample_str)
        samples.append(sample)
        
        config['starting_position'] = event.offset

        await partition_context.update_checkpoint(event)
    else:
        samples.reverse() # most recent in the top of dataset
        global receive
        receive = False

async def close():
    await client.close()

async def main():
    recv_task = asyncio.ensure_future(client.receive(
        on_event=on_event,
        starting_position=config['starting_position'],
        max_wait_time=config['max_wait_time']
    ))

    while(True):
        await asyncio.sleep(3) 
        if(receive != True):
            recv_task.cancel()
            break;

def save_offset():
    last_offset = config['starting_position']
    if last_offset != STARTING_POSITION:
        print(f'{len(samples)} new samples')
        print((f'old offset: {STARTING_POSITION}, new offset: {last_offset}'))
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
        print('Offset saved!')
    else:
        print('0 new samples')

def extract():
    asyncio.run(main())
    asyncio.run(close())
    save_offset()
    return samples

if __name__ == "__main__":
    data = extract()
    # print(data)
