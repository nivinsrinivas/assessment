import yaml


def extract_data(spark, file_path):
    """Load data from CSV file format.
    :param spark: Spark session object.
    :param file_path: CSV File path
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .csv(file_path, header=True, inferSchema=True)
        )

    return df


def load_data_to_csv(df, file_path):
    """Collect data locally and write to file.
    :param df: DataFrame to print.
    :param file_path: Output path to write DF.
    :return: None
    """
    df.coalesce(1).write.mode('overwrite').format("parquet").option("header", "true").save(file_path)
    return None


def read_config(config_file_path):
    """
    Read the config file and get configs as dictionary
    :param config_file_path: file path to config.yaml
    :return: dictionary 'data' with the configs
    """
    with open(config_file_path, "r") as file:
        data = yaml.safe_load(file)
    return data