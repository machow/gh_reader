import yaml

from importlib.resources import files


SCHEMA_TYPES = {
    "integer": "int64",
    "float": "float64",
    "boolean": "bool_",
    "datetime": "timestamp",
    "string": "string",
}

def create_arrow_schema(yml_config: dict):
    import pyarrow as pa

    schema_fields = []
    for name, type_ in yml_config.items():
        type_name = SCHEMA_TYPES[type_]
        if type_name == "timestamp":
            pa_type = pa.timestamp("s")
        else:
            pa_type = getattr(pa, type_name)()
        schema_fields.append(pa.field(name, pa_type))

    return pa.schema(schema_fields)


def to_parquet(schema_name: str, data: str, out_name: str):
    import pyarrow as pa
    from pyarrow import parquet
    from pyarrow import json

    p_schema = files("gh_api") / "schemas" / f"{schema_name}.yml"
    yml_config = yaml.safe_load(open(p_schema))

    schema = create_arrow_schema(yml_config)

    try:
        table = json.read_json(
            data,
            parse_options = json.ParseOptions(explicit_schema=schema)
        )
    except pa.ArrowInvalid as e:
        if "Empty JSON file" in e.args[0]:
            table = pa.Table.from_pylist([], schema)
        else:
            raise e

    parquet.write_table(table, out_name)
