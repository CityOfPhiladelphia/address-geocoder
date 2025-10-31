import yaml, polars as pl, requests
from utils.parse_address import find_address_fields, parse_address
from utils.ais_lookup import split_geos, ais_lookup
from utils.db_lookup import append
from mapping.ais_properties_fields import fields
from passyunk.parser import PassyunkParser
from pathlib import PurePath

def build_append_fields(config: dict) -> tuple[list, list]:
    """
    Given a config dictionary, returns two lists of fields to be
    appended to the input file. One list is the address file fieldnames,
    the other is the AIS fieldnames.

    Args: 
        config (dict): A dictionary read from the config yaml file

    Returns: A tuple with AIS fieldnames and address file fieldnames.
    """
    ais_append_fields = config['append_fields']
    
    invalid_fields = [
        item for item in ais_append_fields if item not in fields.keys()]
    
    if invalid_fields:
        to_print = ", ".join(field for field in invalid_fields)
        raise ValueError("The following fields are not available for append:"
                         f"{to_print}. Please correct these and try again.")
    
    address_file_fields = []

    [address_file_fields.append(fields[item]) for item in ais_append_fields]

    # Need street_address for joining
    address_file_fields.extend(['street_address', 'geocode_lat', 'geocode_lon'])

    return (ais_append_fields, address_file_fields)

def append_address_file_fields(
        geo_filepath: str, input_data: pl.LazyFrame, address_fields: list) -> pl.LazyFrame:
    
    """
    Given a list of address fields to append, appends those fields from
    the address file to each record in the input data. Does so via a
    left join on the full address.
    """
    addresses = pl.scan_parquet(geo_filepath)
    addresses = addresses.select(address_fields)

    rename_mapping = {value: key for key, value in fields.items()
                      if value in address_fields}

    joined_lf = input_data.join(
        addresses, how="left", 
        left_on="output_address", 
        right_on="street_address").rename(rename_mapping)
    
    return joined_lf


def process_csv(config_path) -> pl.LazyFrame:
    """
    Given a config file with the csv filepath, normalizes records
    in that file using Passyunk.

    Args:
        config_path (str): The path to the config file
        chunksize (int): Batch size for file reading

    Returns: A polars lazy dataframe
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    filepath = config.get("input_file")
    geo_filepath = config.get("geography_file")

    if not filepath:
        raise ValueError("An input filepath must be specified in the config " "file.")

    if not geo_filepath:
        raise ValueError(
            "A filepath for the geography file must be" "specified in the config."
        )

    # Determine which fields in the file are the address fields
    address_fields = find_address_fields(config_path)

    p = PassyunkParser()

    lf = pl.scan_csv(filepath, row_index_name="__geocode_idx__")

    # Concatenate address fields, strip extra spaces
    lf = lf.with_columns(
        pl.concat_str(
            [pl.col(field).fill_null("") for field in address_fields], separator=" "
        )
        .str.replace_all(r"\s+", " ")
        .alias("joined_address")
    )
    # Create struct of columns to be filled by parse address function
    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
        ]
    )

    lf = lf.with_columns(
        pl.col("joined_address")
        .map_elements(lambda s: parse_address(p, s), return_dtype=new_cols)
        .alias("temp_struct")
    ).unnest("temp_struct")

    # Generate the names of columns to append for both the AIS API
    # and the address file
    ais_append, address_file_append = build_append_fields(config)

    joined_lf = append_address_file_fields(geo_filepath, lf, address_file_append)

    # Split out fields that did not match the address file
    # and attempt to match them with the AIS API
    has_geo, needs_geo = split_geos(joined_lf)

    new_cols = pl.Struct(
        [
            pl.Field("output_address", pl.String),
            pl.Field("is_addr", pl.Boolean),
            pl.Field("is_philly_addr", pl.Boolean),
            pl.Field("geocode_lat", pl.String),
            pl.Field("geocode_lon", pl.String),
            *[pl.Field(field, pl.String) for field in ais_append]
        ]
    )

   # new_cols.extend([pl.Field(field) for field in ais_append])

    API_KEY = config.get("AIS_API_KEY")

    field_names = [f.name for f in new_cols.fields]

    with requests.Session() as sess:
        needs_geo = (
            needs_geo.with_columns(
                pl.col("output_address")
                .map_elements(
                    lambda a: ais_lookup(sess, API_KEY, a, ais_append), return_dtype=new_cols
                )
                .alias("temp_struct")
            )
            .with_columns(
                *[pl.col("temp_struct").struct.field(n).alias(n) for n in field_names]
            ).drop(
                "temp_struct"
            )
        )

    rejoined = (
        pl.concat([has_geo, needs_geo]).sort("__geocode_idx__").drop("__geocode_idx__")
    )

    in_path = PurePath(filepath)

    # If filepath has multiple suffixes, remove them
    stem = in_path.name.replace("".join(in_path.suffixes), "")

    out_path = f"{stem}_appended.csv"

    out_path = str(in_path.parent / out_path)

    rejoined.sink_csv(out_path)
