import argparse
import collections.abc
import csv
import datetime
import decimal
import json
import random
import sys
import time
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


def main(argv: argparse.Namespace) -> None:
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


def _datetime(fmt: str = default_datetime_format) -> Callable[[str], datetime.datetime]:
    def wrapper(value: str) -> datetime.datetime:
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError as e:
            print(e)
            raise argparse.ArgumentTypeError("bad time") from e

    return wrapper


if __name__ == "__main__":
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
    main(args)
