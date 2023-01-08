### DATASET MODE ###

# Define a function that simulates data for the sensors - three parameters can be specified:
    # "repetitions" refers to the number of times that one complete activity is repeated
    # "anomaly_n_probability" is the likelihood of an anomaly occuring - this can be specified for each of the three sensors
    # "out_of_range_anomaly_share" is the share of anomalies (among all anomalies) that have values outside the defined sensor value range (as opposed to in-range values that are anomalies because they don't follow the usual pattern)
    # "start_time" is the timestamp of the first data point - if none is specified, the current timestamp is used
    
def simulate_dataset(repetitions, anomaly_1_probability, anomaly_2_probability, anomaly_3_probability, out_of_range_anomaly_share, start_time = None):
    # Import required packages 
    import pandas as pd
    from numpy import random as r
    import random
    from datetime import datetime

    # If no start time is specified, take the current time
    if start_time is None:
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Define the closest proximity of the activity, which remains constant while the actual work is carried out
    closest_proximity = r.randint(4, 7)
    activity_steps = ()

    for i in range (0, repetitions):
        # Define the correct values of the three sensors during one activity
        activity_steps += (
            (r.randint(28, 31), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (r.randint(21, 26), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (r.randint(15, 21), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (r.randint(8, 12), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (closest_proximity, r.randint(1100, 1601), r.randint(2001, 2501), "correct"),
            (closest_proximity, r.randint(3000, 3501), r.randint(2800, 3301), "correct"),
            (closest_proximity, r.randint(6100, 6301), r.randint(5100, 5501), "correct"),
            (closest_proximity, r.randint(6250, 6301), r.randint(5300, 5501), "correct"),
            (r.randint(8, 13), r.randint(4000, 4501), r.randint(2900, 3401), "correct"),
            (r.randint(16, 22), r.randint(2000, 2501), r.randint(2200, 2701), "correct"),
            (r.randint(23, 27), r.randint(1500, 1901), r.randint(1800, 2001), "correct"),
            (r.randint(29, 31), r.randint(560, 651), r.randint(1670, 1801), "correct"),
        )

    # Create a dataframe
    df = pd.DataFrame(activity_steps, columns=["Proximity", "Engine Speed", "Pressure", "Label"])

    # Introduce fluctuation into the data
    for i in df.index:
            if(df.at[i, 'Proximity'] != closest_proximity):
                df.at[i, 'Proximity'] = round(df.at[i, 'Proximity'] - r.random(), 2)
            else:
                # Smaller fluctuation for the closest proximity that should remain relatively constant
                df.at[i, 'Proximity'] = round(df.at[i, 'Proximity'] - (r.random()/20), 2)
            df.at[i, 'Engine Speed'] = round(df.at[i, 'Engine Speed'] * (110 - r.randint(0, 21))/100, 0)
            df.at[i, 'Pressure'] = round(df.at[i, 'Pressure'] * (110 - r.randint(0, 21))/100, 0)

    # Add anomalies depending on the probability specified when calling the function
    correct_probability = 1-(anomaly_1_probability+anomaly_2_probability+anomaly_3_probability)
    for i in df.index:
        choice = random.choices(["correct", "proximity_anomaly", "engine_speed_anomaly", "pressure_anomaly"], weights=[correct_probability, anomaly_1_probability, anomaly_2_probability, anomaly_3_probability], k=1)
        match choice[0]:
            case "proximity_anomaly":
                df.at[i, 'Proximity'] = random.choices([round(r.randint(3, 31) - r.random(), 2), round(r.randint(0, 999) - r.random(), 2)], weights=[1-out_of_range_anomaly_share, out_of_range_anomaly_share], k=1)
                df.at[i, 'Label'] = "anomaly"
            case "engine_speed_anomaly":
                df.at[i, 'Engine Speed'] = random.choices([round(r.randint(500, 7001) * (110 - r.randint(0, 21))/100, 0), round(r.randint(0, 99999) - r.random(), 2)], weights=[1-out_of_range_anomaly_share, out_of_range_anomaly_share], k=1)
                df.at[i, 'Label'] = "anomaly"
            case "pressure_anomaly":
                df.at[i, 'Pressure'] = random.choices([round(r.randint(1500, 6001) * (110 - r.randint(0, 21))/100, 0), round(r.randint(0, 99999) - r.random(), 2)], weights=[1-out_of_range_anomaly_share, out_of_range_anomaly_share], k=1)
                df.at[i, 'Label'] = "anomaly"

    # Add the timestamps
    df["Time"] = pd.date_range(start_time, periods=len(df), freq="5s")

    # Rearrange the columns of the dataframe
    df = df[["Time", "Proximity", "Engine Speed", "Pressure", "Label"]]

    # Return the dataframe
    return df

# simulate_dataset(100, 0.01, 0.01, 0.01, 0.5, "2022-01-01 00:00:00")
# df.to_csv("filename.csv", index=False)



### REAL-TIME MODE ###

# Define a function that simulates data for the sensors - these parameters can be specified:
    # "anomaly_n_probability" is the likelihood of an anomaly occuring - this can be specified for each of the three sensors
    # "out_of_range_anomaly_share" is the share of anomalies (among all anomalies) that have values outside the defined sensor value range (as opposed to in-range values that are anomalies because they don't follow the usual pattern)
    # "start_time" is the timestamp of the first data point - if none is specified, the current timestamp is used

def simulate_real_time(anomaly_1_probability, anomaly_2_probability, anomaly_3_probability, out_of_range_anomaly_share, start_time = None):
    # Import required packages 
    import pandas as pd
    from numpy import random as r
    import random
    from time import sleep
    from datetime import datetime
    import sys

    # If no start time is specified, take the current time
    if start_time is None:
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Set repetitions to the largest possible number (for endless execution of "real-time mode")
    repetitions = sys.maxsize

    # Define the closest proximity of the activity, which remains constant while the actual work is carried out
    closest_proximity = 6
    activity_steps = ()

    for n in range (0, repetitions):
        # Define the correct values of the three sensors during one activity
        activity_steps = (
            (r.randint(28, 31), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (r.randint(21, 26), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (r.randint(15, 21), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (r.randint(8, 12), r.randint(560, 651), r.randint(1670, 1801), "correct"),
            (closest_proximity, r.randint(1100, 1601), r.randint(2001, 2501), "correct"),
            (closest_proximity, r.randint(3000, 3501), r.randint(2800, 3301), "correct"),
            (closest_proximity, r.randint(6100, 6301), r.randint(5100, 5501), "correct"),
            (closest_proximity, r.randint(6250, 6301), r.randint(5300, 5501), "correct"),
            (r.randint(8, 13), r.randint(4000, 4501), r.randint(2900, 3401), "correct"),
            (r.randint(16, 22), r.randint(2000, 2501), r.randint(2200, 2701), "correct"),
            (r.randint(23, 27), r.randint(1500, 1901), r.randint(1800, 2001), "correct"),
            (r.randint(29, 31), r.randint(560, 651), r.randint(1670, 1801), "correct"),
        )

        # Create a dataframe
        df = pd.DataFrame(activity_steps, columns=["Proximity", "Engine Speed", "Pressure", "Label"])

        # Introduce fluctuation into the data
        for i in df.index:
                if(df.at[i, 'Proximity'] != closest_proximity):
                    df.at[i, 'Proximity'] = round(df.at[i, 'Proximity'] - r.random(), 2)
                else:
                    # Smaller fluctuation for the closest proximity that should remain relatively constant
                    df.at[i, 'Proximity'] = round(df.at[i, 'Proximity'] - (r.random()/20), 2)
                df.at[i, 'Engine Speed'] = round(df.at[i, 'Engine Speed'] * (110 - r.randint(0, 21))/100, 0)
                df.at[i, 'Pressure'] = round(df.at[i, 'Pressure'] * (110 - r.randint(0, 21))/100, 0)

        # Add anomalies depending on the probability specified when calling the function
        correct_probability = 1-(anomaly_1_probability+anomaly_2_probability+anomaly_3_probability)
        for i in df.index:
            choice = random.choices(["correct", "proximity_anomaly", "engine_speed_anomaly", "pressure_anomaly"], weights=[correct_probability, anomaly_1_probability, anomaly_2_probability, anomaly_3_probability], k=1)
            match choice[0]:
                case "proximity_anomaly":
                    df.at[i, 'Proximity'] = random.choices([round(r.randint(3, 31) - r.random(), 2), round(r.randint(0, 999) - r.random(), 2)], weights=[1-out_of_range_anomaly_share, out_of_range_anomaly_share], k=1)
                    df.at[i, 'Label'] = "anomaly"
                case "engine_speed_anomaly":
                    df.at[i, 'Engine Speed'] = random.choices([round(r.randint(500, 7001) * (110 - r.randint(0, 21))/100, 0), round(r.randint(0, 99999) - r.random(), 2)], weights=[1-out_of_range_anomaly_share, out_of_range_anomaly_share], k=1)
                    df.at[i, 'Label'] = "anomaly"
                case "pressure_anomaly":
                    df.at[i, 'Pressure'] = random.choices([round(r.randint(1500, 6001) * (110 - r.randint(0, 21))/100, 0), round(r.randint(0, 99999) - r.random(), 2)], weights=[1-out_of_range_anomaly_share, out_of_range_anomaly_share], k=1)
                    df.at[i, 'Label'] = "anomaly"

        # Add the timestamps      
        df["Time"] = pd.date_range(start_time, periods=len(df), freq="5s")

        # Set the start time for the next repetition of the process
        start_time = df["Time"].iloc[-1] + pd.Timedelta("5 seconds")

        # Rearrange the columns of the dataframe ("Label" is not used here as it's not need by the real-time mode)
        df = df[["Time", "Proximity", "Engine Speed", "Pressure"]]

        # Continuously return single data measurements in "real-time mode"
        for x in range(0, len(df)):
            yield(df.loc[x:x])
            sleep(5)

# for i in simulate_real_time(0.01, 0.01, 0.01, 0.5):
#     print(i.to_string(index = False))