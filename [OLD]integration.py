# Digital Twin Updates & Queries
import azure.identity as ident
import azure.digitaltwins.core as dt

# URL to the Azure Digital Twin
twin_url = "https://RL-Resource-Instance.api.eus.digitaltwins.azure.net"

# DefaultAzureCredential supports different authentication mechanisms and determines the appropriate credential type based of the environment it is executing in.
# It attempts to use multiple credential types in an order until it finds a working credential.
# DefaultAzureCredential expects the following three environment variables:
# - AZURE_TENANT_ID: The tenant ID in Azure Active Directory
# - AZURE_CLIENT_ID: The application (client) ID registered in the AAD tenant
# - AZURE_CLIENT_SECRET: The client secret for the registered application

credential = ident.DefaultAzureCredential()
service_client = dt.DigitalTwinsClient(twin_url, credential)
twin_id = "aluminum_packaging_process_twin"

# Patches (= replacing or adding values for properties of components)

# Function to set engine speed
def set_engine_speed(value):
    component_1 = "cncEngineSpeedSensor"
    try:
        patch_1 = [
            {
                "op": "replace",
                "path": "/engineSpeed",
                "value": value
            }
        ]
        service_client.update_component(twin_id, component_1, patch_1)
    except:
        patch_1 = [
            {
                "op": "add",
                "path": "/engineSpeed",
                "value": value
            }
        ]
        service_client.update_component(twin_id, component_1, patch_1)


# Function to set pressure
def set_pressure(value):
    component_2 = "cncPressureSensor"
    try:
        patch_2 = [
            {
                "op": "replace",
                "path": "/pressure",
                "value": value
            }
        ]
        service_client.update_component(twin_id, component_2, patch_2)
    except:
        patch_2 = [
            {
                "op": "add",
                "path": "/pressure",
                "value": value
            }
        ]
        service_client.update_component(twin_id, component_2, patch_2)


# Function to set proximity
def set_proximity(value):
    component_3 = "cncProximitySensor"
    try:
        patch_3 = [
            {
                "op": "replace",
                "path": "/proximity",
                "value": value
            }
        ]
        service_client.update_component(twin_id, component_3, patch_3)
    except:
        patch_3 = [
            {
                "op": "add",
                "path": "/proximity",
                "value": value
            }
        ]
        service_client.update_component(twin_id, component_3, patch_3)


# Query that shows all details of our digital twin
# query_expression = "SELECT * FROM digitaltwins WHERE $dtId = 'aluminum_packaging_process_twin'"
# query_result = service_client.query_twins(query_expression)
# print('Aluminum packaging process twin:')
# for twin in query_result:
#     print(twin)

# # Set engine speed: 500-7000 RPM (Integer)
# set_engine_speed(7000)

# # Set pressure: 1500-6000 bar (Integer)
# set_pressure(6000)

# # Set proximity: 3.0-30.0 millimetres (Float)
# set_proximity(30.0)

def get_engine_speed():
    get_component = service_client.get_component(twin_id, "cncEngineSpeedSensor")
    return get_component["engineSpeed"]


def print_engine_speed():
    get_component = service_client.get_component(twin_id, "cncEngineSpeedSensor")
    print('Engine speed: ' + str(get_component["engineSpeed"]) + 'RPM (updated: ' + str(get_component["$metadata"]["$lastUpdateTime"]) + ")")


def get_pressure():
    get_component = service_client.get_component(twin_id, "cncPressureSensor")
    return get_component["pressure"]


def print_pressure():
    get_component = service_client.get_component(twin_id, "cncPressureSensor")
    print('Pressure: ' + str(get_component["pressure"]) + 'bar (updated: ' + str(get_component["$metadata"]["$lastUpdateTime"]) + ")")


def get_proxmity():
    get_component = service_client.get_component(twin_id, "cncProximitySensor")
    return get_component["proximity"]


def print_proxmity():
    get_component = service_client.get_component(twin_id, "cncProximitySensor")
    print('Proximity: ' + str(get_component["proximity"]) + 'mm (updated: ' + str(get_component["$metadata"]["$lastUpdateTime"]) + ")")


# Digital Twin Visualization
import matplotlib.pyplot as plt
from IPython.display import clear_output

def plot_sensor_values():
    # Get current sensor values
    engine_speed = get_engine_speed()
    pressure = get_pressure()
    proximity = get_proxmity()

    # Create subplots
    plt.rcParams["figure.figsize"] = (12,7)
    figure, axis = plt.subplots(1, 3)
    plt.subplots_adjust(wspace=1)
    plt.suptitle("Digital Twin")

    # Plot 1
    axis[0].bar(x=0, height=engine_speed, width=1, align='center', alpha=1, color="#22a7f0")
    axis[0].set_xticks([])
    axis[0].set_xlabel('Engine speed')
    axis[0].set_ylabel('RPM')
    axis[0].set_yticks([0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000])

    # Plot 2
    axis[1].bar(x=0, height=pressure, width=1, align='center', alpha=1, color="#e14b31")
    axis[1].set_xticks([])
    axis[1].set_xlabel('Pressure')
    axis[1].set_ylabel('bar')
    axis[1].set_yticks([0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000])

    # Plot 3
    axis[2].bar(x=0, height=proximity, width=1, align='center', alpha=1, color="#76c68f")
    axis[2].set_xticks([])
    axis[2].set_xlabel('Proximity')
    axis[2].set_ylabel('mm')
    axis[2].set_yticks([0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30])

    # Show all plots
    plt.show()
    # clear_output(wait=True)


# Simulation
import argparse
import collections.abc
import csv
import datetime
import decimal
import json
import random
import sys
import time
import psutil
import os
from typing import Callable, Mapping, NamedTuple, Any, Sequence, List

random.seed(time.time())

default_datetime_format = "%d.%m.%Y %H:%M"

config_schema = {
    "$schema": "http://json-schema.org/schema#",
    "type": "object",
    "properties": {
        "metrics": {
            "type": "object",
            "patternProperties": {
                ".*": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "minimum": {"type": "number"},
                                "maximum": {"type": "number"},
                            },
                            "required": ["minimum", "maximum"]
                        },
                        {
                            "type": "array",
                            "minItems": 1
                        }
                    ]
                }
            }
        }
    },
    "required": ["metrics"]
}


class Metric(NamedTuple):
    name: str
    generator: Callable[[], Any]


def _get_metrics(config: Mapping) -> Sequence[Metric]:
    metrics: Mapping = config["metrics"]
    list_of_metrics: List[Metric] = []

    for name, metric in metrics.items():
        if isinstance(metric, collections.abc.Sequence):
            def generator(metric_value: Sequence = metric) -> Any:
                return random.choice(metric_value)
        else:
            minimum, maximum = metric["minimum"], metric["maximum"]
            is_float = isinstance(minimum, float) or isinstance(maximum, float)
            if not is_float:
                def generator(min_value: int = minimum, max_value: int = maximum) -> int:
                    return min_value + random.randint(0, max_value - min_value)
            else:
                exp = max(
                    abs(decimal.Decimal(str(minimum)).as_tuple().exponent),
                    abs(decimal.Decimal(str(maximum)).as_tuple().exponent)
                )

                def generator(min_value: float = float(minimum),
                              max_value: float = float(maximum),
                              exp_value: int = exp) -> float:
                    return round(min_value + (max_value - min_value) * random.random(), exp_value)

        list_of_metrics.append(Metric(name=name, generator=generator))

    return list_of_metrics


def main(argv: argparse.Namespace, method) -> None:
    if method == "output":
        try:
            from jsonschema import validate
        except ImportError:
            def validate(*_):
                pass
        config_data = json.load(argv.config_file)
        validate(config_data, config_schema)
        metrics = _get_metrics(config_data)
        writer = csv.writer(sys.stdout)
        writer.writerow((
            "date",
            *map(lambda m: m.name, metrics
        )))
        delta = datetime.timedelta(seconds=argv.update_interval)
        curr_time = argv.start_time or datetime.datetime.now()
        end_time = argv.end_time
        while end_time is None or curr_time <= end_time:
            row = (
                curr_time.strftime(default_datetime_format),
                *map(lambda m: m.generator(), metrics)
            )
            writer.writerow(row)
            if end_time is None:
                time.sleep(delta.seconds)
            curr_time += delta
    elif method == "send_to_twin":
        try:
            from jsonschema import validate
        except ImportError:
            def validate(*_):
                pass
        config_data = json.load(argv.config_file)
        validate(config_data, config_schema)
        metrics = _get_metrics(config_data)
        
        delta = datetime.timedelta(seconds=argv.update_interval)
        curr_time = argv.start_time or datetime.datetime.now()
        end_time = argv.end_time
        while end_time is None or curr_time <= end_time:
            row = (
                curr_time.strftime(default_datetime_format),
                *map(lambda m: m.generator(), metrics)
            )
            proxmity = row[1]
            engine_speed = row[2]
            pressure = row[3]

            set_proximity(proxmity)
            set_engine_speed(engine_speed)
            set_pressure(pressure)

            print_proxmity()
            print_engine_speed()
            print_pressure()

            plot_sensor_values()
            
            if end_time is None:
                time.sleep(delta.seconds)
            curr_time += delta


def _datetime(fmt: str = default_datetime_format) -> Callable[[str], datetime.datetime]:
    def wrapper(value: str) -> datetime.datetime:
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError as e:
            print(e)
            raise argparse.ArgumentTypeError("bad time") from e

    return wrapper


ppid = os.getppid()
pname = psutil.Process(ppid).name()


# Function call if the script is run from within a Jupyter Notebook
if (pname == "Python") & (__name__ == "__main__"):
    parser = argparse.ArgumentParser(prog="Data Generator")
    parser.add_argument("-c", "--config", dest="config_file", type=argparse.FileType(), required=True,
                        help="Data configuration file.")
    parser.add_argument("-i", "--update-interval", dest="update_interval", type=int, default=15 * 60,
                        help="Update interval (sec).")
    parser.add_argument("-s", "--start-time", dest="start_time", type=_datetime(),
                        help="Start time.")
    parser.add_argument("-e", "--end-time", dest="end_time", type=_datetime(),
                        help="Start time.")
    args = parser.parse_args(['--config', 'config.json', '--update-interval', '900'])
    # Method: "output" or "send_to_twin"
    main(args, method = "output")
# Function call if the script is run from the command line
elif __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Data Generator")
    parser.add_argument("-c", "--config", dest="config_file", type=argparse.FileType(), required=True,
                        help="Data configuration file.")
    parser.add_argument("-i", "--update-interval", dest="update_interval", type=int, default=15 * 60,
                        help="Update interval (sec).")
    parser.add_argument("-s", "--start-time", dest="start_time", type=_datetime(),
                        help="Start time.")
    parser.add_argument("-e", "--end-time", dest="end_time", type=_datetime(),
                        help="Start time.")
    args = parser.parse_args()
    # Method: "output" or "send_to_twin"
    main(args, method = "output")

# Example command line calls:
# python3 integration.py --config "config.json"
# python3 integration.py --config "config.json" --update-interval 2
# python3 integration.py --config "config.json" --update-interval 900 --start-time "01.01.2022 00:00" --end-time "31.12.2022 23:45"