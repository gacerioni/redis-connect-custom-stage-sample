# DUMMY SCRIPT WITH HARDCODED STUFF
# IT HELPS ME TO COMPARE RDI AND CONNECT RTT TO REACT TO AN SQL UPDATE ON
# UPDATE CHINOOK.TRACK SET NAME = 'GABS', GENREID = 2 WHERE TRACKID = 140;
# commit;
# SELECT * FROM CHINOOK.TRACK WHERE TRACKID = 140;


import time
import redis
import json

# Configuration for both Redis instances
config = {
    "RedisConnect": {
        "host": "redis-14000.redis.platformengineer.io",
        "port": 14000,
        "password": "blabla",
        "key": "TRACK:140",
    },
    "RDI": {
        "host": "redis-12004.redis.platformengineer.io",
        "port": 12004,
        "password": "blabla",
        "key": "track:TRACKID:140",
    },
}

# Redis client setup
clients = {
    name: redis.StrictRedis(
        host=config[name]["host"],
        port=config[name]["port"],
        password=config[name]["password"],
        decode_responses=True
    )
    for name in config
}

# Function to get the NAME value from JSON
def get_name_value(client, key):
    try:
        response = client.execute_command("JSON.GET", key, "RETURN", 1, "$.NAME")
        parsed_response = json.loads(response)
        name_value = parsed_response.get("$.NAME", [None])[0]
        return name_value
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# Monitoring both Redis instances for a NAME change
def monitor():
    detection_times = {}
    initial_values = {name: get_name_value(client, config[name]["key"]) for name, client in clients.items()}

    print("Initial NAME values:", initial_values)
    print("Monitoring for any NAME change...")

    while len(detection_times) < 2:
        for name, client in clients.items():
            if name not in detection_times:
                current_value = get_name_value(client, config[name]["key"])
                if current_value is not None and current_value != initial_values[name]:
                    detection_times[name] = time.time()
                    print(f"{name} detected a NAME change to '{current_value}' at {detection_times[name]} seconds.")
                    initial_values[name] = current_value  # Update initial value for future changes

        time.sleep(0.1)  # Check every 100 ms to reduce load

    # Calculate and display the time difference
    time_diff = abs(detection_times["RDI"] - detection_times["RedisConnect"]) * 1000  # ms
    faster = "RDI" if detection_times["RDI"] < detection_times["RedisConnect"] else "RedisConnect"
    print(f"{faster} was faster by {time_diff:.2f} ms")

if __name__ == "__main__":
    monitor()
