import csv
import datetime
import decimal
import random
import sys
from typing import Sequence, Union

# Lines before "begin" can be changed. All lines below not.

proximity_min = 3.0
proximity_max = 30.0

engine_speed_min = 500
engine_speed_max = 7000

pressure_min = 1500
pressure_max = 6000

# Percentage of anomalies among all measurements
anomaly_percent = 2

start_time = "09.12.2022 18:00:00"
end_time = "09.12.2022 18:02:00"
interval = 1

# Relative share of the different sensors among the anomalies set above
anomalies_distribution = (33, 33, 34)  # 33% proximity, 33% engine speed, 34% pressure

exp = 3

# begin

number_of_patterns = 3

assert sum(anomalies_distribution) == 100

distribution = (
    (0, anomalies_distribution[0]),
    (anomalies_distribution[0], anomalies_distribution[0] + anomalies_distribution[1]),
    (anomalies_distribution[1], anomalies_distribution[1] + anomalies_distribution[2])
)

_time_fmt = "%d.%m.%Y %H:%M:%S"


def get_random_int(min_v: int, max_v: int) -> int:
    return min_v + random.randint(0, max_v - min_v)


def get_random_float(min_v: float, max_v: float) -> float:
    exp = max(
        abs(decimal.Decimal(str(min_v)).as_tuple().exponent),
        abs(decimal.Decimal(str(max_v)).as_tuple().exponent)
    )
    return round(min_v + (max_v - min_v) * random.random(), exp)


def calculate_anomaly_times(
    start: datetime.datetime,
    duration_in_sec: int,
    number_of_measurements: int,
    percent_of_anomalies: int
) -> Sequence[datetime.datetime]:
    if number_of_measurements == 0:
        number_of_measurements = 1
    total_anomalies = int(float(number_of_measurements) * percent_of_anomalies / 100)
    if total_anomalies == 0:
        total_anomalies = 1
    anomaly_interval = duration_in_sec // total_anomalies
    intervals = [(n + 1) * anomaly_interval for n in range(total_anomalies)]
    for i in range(len(intervals) - 1):
        curr_interval, next_interval = intervals[i], intervals[i + 1]
        interval_diff = next_interval - curr_interval
        delta_value = int(interval_diff / 5)
        if random.randint(1, 0xFFFFFFFF) & 1:
            next_interval += delta_value
        else:
            next_interval -= delta_value
        intervals[i + 1] = next_interval
    times = tuple(start + datetime.timedelta(seconds=s) for s in intervals)
    return times


def split(start_v: Union[int, float], end_v: Union[int, float], count: int) -> Sequence[Union[int, float]]:
    if start_v < end_v:
        begin, end = start_v, end_v
    else:
        begin, end = end_v, start_v
    diff = end - begin
    delta = diff / count
    intervals = [begin + (n + 1) * delta for n in range(count)]
    for i in range(len(intervals) - 1):
        curr_i, next_i = intervals[i], intervals[i + 1]
        interval_diff = next_i - curr_i
        delta_value = int(interval_diff * random.randint(1, 99) / 100)
        if random.randint(1, 0xFFFFFFFF) & 1:
            next_i += delta_value
        else:
            next_i -= delta_value
        intervals[i + 1] = next_i
    return tuple(intervals if begin == start_v else reversed(intervals))


start_dt = datetime.datetime.strptime(start_time, _time_fmt)
end_dt = datetime.datetime.strptime(end_time, _time_fmt)
measurements_time: datetime.timedelta = end_dt - start_dt
duration = int(measurements_time.total_seconds())
measurements_count = duration // interval

anomaly_times = calculate_anomaly_times(start_dt, duration, measurements_count, anomaly_percent)

pattern_period = duration // number_of_patterns
pattern_1_end_time = start_dt + datetime.timedelta(seconds=pattern_period)
pattern_2_end_time = pattern_1_end_time + datetime.timedelta(seconds=pattern_period)

curr_proximity = get_random_float(proximity_max - (1 + random.random()), proximity_max)
proximity_at_end_of_pattern_1 = None
curr_engine_speed = get_random_int(
    engine_speed_min,
    engine_speed_min + int(float(engine_speed_max - engine_speed_min) * 1 / 100)
)
curr_pressure = get_random_int(pressure_min, pressure_min + int(float(pressure_max - pressure_min) * 1 / 100))

measurements_within_pattern = pattern_period // interval
proximity_values_in_pattern_1 = split(curr_proximity, proximity_min, measurements_within_pattern + 1)
engine_speed_values_in_pattern_2 = None
engine_speed_index_in_pattern_2 = 0
pressure_values_in_pattern_2 = None
pressure_index_in_pattern_2 = 0
engine_speed_values_in_pattern_3 = None
engine_speed_index_in_pattern_3 = 0
pressure_values_in_pattern_3 = None
pressure_index_in_pattern_3 = 0
proximity_values_in_pattern_3 = None
proximity_index_in_pattern_3 = 0

curr_dt = start_dt
anomaly_index = 0
proximity_index_in_pattern_1 = 0

with open('out.csv', 'w', newline='') as csvfile:

    # writer = csv.writer(sys.stdout)
    writer = csv.writer(csvfile)
    prev_proximity = None
    prev_engine_speed = None
    prev_pressure = None

    while curr_dt < end_dt:
        pattern: int
        if curr_dt <= pattern_1_end_time:
            pattern = 1
            curr_proximity = proximity_values_in_pattern_1[proximity_index_in_pattern_1]
            proximity_index_in_pattern_1 += 1
            curr_engine_speed = get_random_int(
                engine_speed_min,
                engine_speed_min + int(float(engine_speed_max - engine_speed_min) * 1 / 100)
            )
            curr_pressure = get_random_int(pressure_min, pressure_min + int(float(pressure_max - pressure_min) * 1 / 100))
        elif curr_dt <= pattern_2_end_time:
            pattern = 2
            if proximity_at_end_of_pattern_1 is None:
                proximity_at_end_of_pattern_1 = curr_proximity
            if engine_speed_values_in_pattern_2 is None:
                engine_speed_values_in_pattern_2 = split(
                    curr_engine_speed,
                    engine_speed_max,
                    measurements_within_pattern + 1
                )
            if pressure_values_in_pattern_2 is None:
                pressure_values_in_pattern_2 = split(
                    curr_pressure,
                    pressure_max,
                    measurements_within_pattern + 1
                )
            curr_proximity = get_random_float(
                proximity_at_end_of_pattern_1 - (1 + random.random()),
                proximity_at_end_of_pattern_1 + (1 + random.random())
            )
            if curr_proximity < proximity_min:
                curr_proximity = proximity_min + random.random()
            curr_engine_speed = engine_speed_values_in_pattern_2[engine_speed_index_in_pattern_2]
            engine_speed_index_in_pattern_2 += 1
            curr_pressure = pressure_values_in_pattern_2[pressure_index_in_pattern_2]
            pressure_index_in_pattern_2 += 1
        else:
            pattern = 3
            if engine_speed_values_in_pattern_3 is None:
                engine_speed_values_in_pattern_3 = split(
                    curr_engine_speed,
                    engine_speed_min,
                    measurements_within_pattern + 1
                )
            if pressure_values_in_pattern_3 is None:
                pressure_values_in_pattern_3 = split(
                    curr_pressure,
                    pressure_min,
                    measurements_within_pattern + 1
                )
            if proximity_values_in_pattern_3 is None:
                proximity_values_in_pattern_3 = split(
                    curr_proximity,
                    proximity_max,
                    measurements_within_pattern + 1
                )
            curr_proximity = proximity_values_in_pattern_3[proximity_index_in_pattern_3]
            proximity_index_in_pattern_3 += 1
            curr_engine_speed = engine_speed_values_in_pattern_3[engine_speed_index_in_pattern_3]
            engine_speed_index_in_pattern_3 += 1
            curr_pressure = pressure_values_in_pattern_3[pressure_index_in_pattern_3]
            pressure_index_in_pattern_3 += 1

        normal_curr_proximity = curr_proximity
        normal_curr_engine_speed = curr_engine_speed
        normal_curr_pressure = curr_pressure

        is_anomaly = False

        if anomaly_times and anomaly_index < len(anomaly_times):
            if curr_dt >= anomaly_times[anomaly_index]:
                is_anomaly = True
                random_0_100 = random.randint(0, 100)
                if random_0_100 < distribution[0][1]:
                    # anomaly in proximity
                    #print("anomaly proximity:", pattern)
                    if pattern == 1:
                        curr_proximity = (prev_proximity or proximity_max) + random.randint(1, 10)
                    elif pattern == 2:
                        curr_proximity = proximity_max + random.randint(1, 10)
                    elif pattern == 3:
                        curr_proximity = (prev_proximity or proximity_min) - random.randint(1, 10)
                elif random_0_100 < distribution[1][1]:
                    # anomaly in engine speed
                    #print("anomaly engine speed:", pattern)
                    if pattern == 1:
                        curr_engine_speed = engine_speed_max + random.randint(1, 10)
                    elif pattern == 2:
                        curr_engine_speed = (prev_engine_speed or proximity_min) - random.randint(1, 10)
                    elif pattern == 3:
                        curr_engine_speed = (prev_engine_speed or proximity_max) + random.randint(1, 10)
                else:
                    # anomaly in pressure
                    #print("anomaly pressure:", pattern)
                    if pattern == 1:
                        curr_pressure = pressure_max + random.randint(1, 10)
                    elif pattern == 2:
                        curr_pressure = (prev_pressure or pressure_min) - random.randint(1, 10)
                    elif pattern == 3:
                        curr_pressure = (prev_pressure or pressure_max) + random.randint(1, 10)

                anomaly_index += 1

        writer.writerow((
            curr_dt.strftime(_time_fmt),
            round(curr_proximity, exp),
            int(curr_engine_speed),
            int(curr_pressure),
            "correct" if not is_anomaly else "anomaly"
        ))

        curr_proximity = normal_curr_proximity
        curr_engine_speed = normal_curr_engine_speed
        curr_pressure = normal_curr_pressure

        curr_dt += datetime.timedelta(seconds=interval)
        prev_proximity = curr_proximity
        prev_engine_speed = curr_engine_speed
        prev_pressure = curr_pressure
