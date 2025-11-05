import time
import random
from datetime import datetime

PATH = "node-textfiles/cpu.prom"

while True:
    temp = round(random.uniform(45, 75), 1)
    battery = random.randint(30, 100)
    charging = random.choice([0, 1])

    with open(PATH, "w") as f:
        f.write(f"# HELP cpu_temperature_celsius Fake CPU temperature\n")
        f.write(f"# TYPE cpu_temperature_celsius gauge\n")
        f.write(f"cpu_temperature_celsius {temp}\n\n")

        f.write(f"# HELP battery_percent Fake laptop battery capacity\n")
        f.write(f"# TYPE battery_percent gauge\n")
        f.write(f"battery_percent {battery}\n\n")

        f.write(f"# HELP battery_charging Fake battery charging (1=charging,0=discharging)\n")
        f.write(f"# TYPE battery_charging gauge\n")
        f.write(f"battery_charging {charging}\n\n")

        f.write(f"# HELP update_timestamp Last fake update time\n")
        f.write(f"# TYPE update_timestamp gauge\n")
        f.write(f"update_timestamp {int(datetime.now().timestamp())}\n")

    print(f"Updated → Temp: {temp}°C | Battery: {battery}% | Charging: {charging}")
    time.sleep(10)
