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


def get_proximity():
    get_component = service_client.get_component(twin_id, "cncProximitySensor")
    return get_component["proximity"]


def print_proximity():
    get_component = service_client.get_component(twin_id, "cncProximitySensor")
    print('Proximity: ' + str(get_component["proximity"]) + 'mm (updated: ' + str(get_component["$metadata"]["$lastUpdateTime"]) + ")")



# Digital Twin Visualization
import matplotlib.pyplot as plt
from IPython.display import clear_output

def plot_sensor_values():
    # Get current sensor values
    engine_speed = get_engine_speed()
    pressure = get_pressure()
    proximity = get_proximity()

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
    clear_output(wait=True)