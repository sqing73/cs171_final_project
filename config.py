# generage a random config.json file
import random
import json
with open("config.json", "r") as f:
    config = json.load(f)
    f.close()

for i in range(5):
    config[str(i+1)] = random.randint(1000, 20000)

with open("config.json", "w") as f:
    json.dump(config, f, indent=4)
    f.close()