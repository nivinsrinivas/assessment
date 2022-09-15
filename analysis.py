"""US Car crash Analysis using Pyspark DataFrame APIs"""
import findspark

findspark.init()

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from utils.helper import extract_data, load_data_to_csv, read_config


class CarCrashAnalysis:
    """Car Crash Analysis class for analysing US Vehicle Car Crash dataset"""

    def __init__(self, path_to_config_file):
        """Read All the CSV files in Data folder which has the car crash info"""
        self.input_file_paths = read_config(path_to_config_file).get("INPUT")

        self.Primary_person_use_df = extract_data(
            spark, self.input_file_paths.get("Primary_Person")
        )
        self.Primary_person_use_df.cache()

        self.Units_use_df = extract_data(spark, self.input_file_paths.get("Units"))
        self.Units_use_df.cache()

        self.Damages_use_df = extract_data(spark, self.input_file_paths.get("Damages"))
        self.Damages_use_df.cache()

        self.Charges_use_df = extract_data(spark, self.input_file_paths.get("Charges"))
        # self.df_endorse = extract_data(spark, self.input_file_paths.get("Endorse"))
        # self.df_restrict = extract_data(spark, self.input_file_paths.get("Restrict"))

    def male_car_crash_analysis(self, output_path):
        """Method to analyze number of accidents involving males"""
        # Gender wise crash count
        # self.Primary_person_use_df.groupBy(col("PRSN_GNDR_ID")).count().show()

        # Filtering the DF where gender is Male
        male_car_crash_df = self.Primary_person_use_df.filter(
            col("PRSN_GNDR_ID") == "MALE"
        )

        # Number of crashes in which person killed is Male
        male_car_crash_count = male_car_crash_df.count()

        # Load output to CSV output in parquet format
        load_data_to_csv(male_car_crash_df, output_path)
        return male_car_crash_count

    def two_wheeler_crash_analysis(self, output_path):
        """Method to analyze number of accidents involving two wheelers"""
        # Vehicle type wise crash count
        # self.Units_use_df.groupBy(col("VEH_BODY_STYL_ID")).count().show(truncate=False)

        # Unit ID wise crash count
        # distinct_unit_id = self.Units_use_df.groupBy(col("UNIT_DESC_ID")).count()
        # distinct_unit_id.show(truncate=False)

        # Filtering the DF where vehicle body type is Motorcycle (2 wheeler)
        two_wheeler_crash_df = self.Units_use_df.filter(
            (
                col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%")
                | (col("UNIT_DESC_ID") == "PEDALCYCLIST")
            )
        )

        # Number of two wheelers booked for crashes
        two_wheeler_crash_count = two_wheeler_crash_df.count()

        # Load output to CSV output in parquet format
        load_data_to_csv(two_wheeler_crash_df, output_path)
        return two_wheeler_crash_count

    def female_car_crash_analysis(self, output_path):
        """
        Method to analyze state that has the highest number of accidents
        involving females
        """
        # State wise accidents with gender filtered tp 'female'
        accident_info_df = (
            self.Primary_person_use_df.filter(col("PRSN_GNDR_ID") == "FEMALE")
            .groupBy(col("DRVR_LIC_STATE_ID"))
            .count()
            .orderBy(col("count").desc())
        )
        # accident_info_df.show(10)

        # State with highest number of accidents
        state_with_most_accidents = accident_info_df.first()["DRVR_LIC_STATE_ID"]

        # Load output to CSV output in parquet format
        load_data_to_csv(accident_info_df, output_path)
        return state_with_most_accidents

    def vehicle_make_crash_analysis(self, output_path):
        """
        Method to analyze vehicle makes from top5 to 15 that
        contributes to largest number of injuries including death
        """
        injury_and_vehicle_info_df = (
            self.Units_use_df.select(
                col("CRASH_ID"),
                col("VEH_MAKE_ID"),
                col("TOT_INJRY_CNT"),
                col("DEATH_CNT"),
            )
            .withColumn(
                "ALL_INJURIES",
                self.Units_use_df["TOT_INJRY_CNT"] + self.Units_use_df["DEATH_CNT"],
            )
            .orderBy(col("ALL_INJURIES").desc())
        )

        # injury_and_vehicle_info_df.show(10)

        vehicle_wise_injuries_df = (
            injury_and_vehicle_info_df.filter(
                injury_and_vehicle_info_df["VEH_MAKE_ID"] != "NA"
            )
            .groupBy("VEH_MAKE_ID")
            .sum("ALL_INJURIES")
            .withColumnRenamed("sum(ALL_INJURIES)", "total_injuries")
            .orderBy(col("total_injuries").desc())
            .limit(15)
        )
        # vehicle_wise_injuries_df.show()
        # Filtering from 5 to 15 rows
        vehicle_wise_injuries_df_5_to_15 = vehicle_wise_injuries_df.tail(11)
        vehicles_list = [
            vehicle_info[0] for vehicle_info in vehicle_wise_injuries_df_5_to_15
        ]

        # Load output to CSV output in parquet format
        load_data_to_csv(vehicle_wise_injuries_df, output_path)
        return vehicles_list

    def vehicle_body_style_crash_analysis(self, output_path):
        """
        Method to analyze vehicle body style involved in crashes, and the top
        ethnic user group of each unique body style
        """
        # We need to join units_use_df and Primary_person_use_df
        vehicle_and_ethnicity_info = self.Primary_person_use_df.join(
            self.Units_use_df,
            self.Primary_person_use_df["CRASH_ID"] == self.Units_use_df["CRASH_ID"],
            how="inner",
        ).select(
            self.Units_use_df["VEH_BODY_STYL_ID"],
            self.Primary_person_use_df["PRSN_ETHNICITY_ID"],
        )
        # vehicle_and_ethnicity_info.show(10, truncate=False)

        # Top ethnic user group of each unique body style
        windowSpec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())

        vehicle_and_ethnicity_df = vehicle_and_ethnicity_info.filter(
            vehicle_and_ethnicity_info["VEH_BODY_STYL_ID"].isin(
                ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
            )
            == False
        ).filter(
            vehicle_and_ethnicity_info["PRSN_ETHNICITY_ID"].isin(["NA", "UNKNOWN"])
            == False
        )
        # vehicle_and_ethnicity_df.show(10, truncate=False)

        vehicle_and_ethnicity_df = (
            vehicle_and_ethnicity_df.groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row_num", row_number().over(windowSpec))
            .filter(col("row_num") == 1)
        )

        vehicle_and_ethnicity_df = vehicle_and_ethnicity_df.select(
            col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID")
        )
        vehicle_and_ethnicity_list = vehicle_and_ethnicity_df.collect()
        # Load output to CSV output in parquet format
        load_data_to_csv(vehicle_and_ethnicity_df, output_path)
        return vehicle_and_ethnicity_list

    def alcohol_related_crash_analysis(self, output_path):
        """
        Method to analyze car crashes where alcohol is a contributing
        factor, returning the top 5 zip codes from the analysis
        """
        crashes_due_to_alcohol_df = (
            self.Units_use_df.join(
                self.Primary_person_use_df,
                self.Units_use_df["CRASH_ID"] == self.Primary_person_use_df["CRASH_ID"],
                how="inner",
            )
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").like("%ALCOHOL%")
                | col("CONTRIB_FACTR_2_ID").like("%ALCOHOL%")
                | (col("PRSN_ALC_RSLT_ID") == "Positive")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        # crashes_due_to_alcohol_df.show()
        # Load output to CSV output in parquet format
        load_data_to_csv(crashes_due_to_alcohol_df, output_path)
        return crashes_due_to_alcohol_df.collect()

    def insurance_related_crash_analysis(self, output_path):
        """
        Method to Count the Distinct Crash IDs where No Damaged
        Property was observed and Damage Level (VEH_DMAG_SCL~)
        is above 4 and car avails Insurance
        """
        damage_insurance_info = self.Damages_use_df.join(
            self.Units_use_df, on=["CRASH_ID"], how="inner"
        )
        # damage_insurance_info.select(col("CRASH_ID"), col("DAMAGED_PROPERTY")).show(10, truncate=False)

        no_damage_insurace_availed_df = (
            damage_insurance_info.filter(
                (
                    (col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4")
                    & (col("VEH_DMAG_SCL_1_ID").isin(["NA", "NO DAMAGE"]) == False)
                )
                | (
                    (col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4")
                    & (col("VEH_DMAG_SCL_2_ID").isin(["NA", "NO DAMAGE"]) == False)
                )
            )
            .filter(
                (col("DAMAGED_PROPERTY") == "NONE")
                | (col("DAMAGED_PROPERTY").like("NO DAMAGE%"))
            )
            .filter(col("FIN_RESP_TYPE_ID").like("%LIABILITY INSURANCE POLICY%"))
        )

        # Load output to CSV output in parquet format
        load_data_to_csv(no_damage_insurace_availed_df, output_path)
        # Number of cars
        return no_damage_insurace_availed_df.count()

    def speeding_related_crash_analysis(self, output_path):
        """
        Method to Determine the Top 5 Vehicle Makes where drivers
        are charged with speeding related offences, has licensed Drivers,
        uses top 10 used vehicle colours and has car licensed with the
        Top 25 states with highest number of offences
        """
        # Top 10 used vehicle colors
        vehicle_colors_df = (
            self.Units_use_df.groupby(col("VEH_COLOR_ID"))
            .count()
            .orderBy(col("count").desc())
        )

        # top 10
        top_ten_vehicle_colors = vehicle_colors_df.limit(10)
        # top_ten_vehicle_colors.show()

        # Filtering null values from top_ten_vehicle_colors
        vehicle_colors_df = vehicle_colors_df.filter(
            (col("VEH_COLOR_ID") == "NA") == False
        )
        top_ten_vehicle_colors = vehicle_colors_df.limit(10)
        # top_ten_vehicle_colors.show()

        # Top 25 states
        vehicle_state_df = (
            self.Units_use_df.groupBy(col("VEH_LIC_STATE_ID"))
            .count()
            .orderBy(col("count").desc())
        )
        # vehicle_state_df.show(25)

        # Filter bad records in vehicle_state_df
        # We only need Vehicle state ID
        vehicle_state_df = vehicle_state_df.filter(
            col("VEH_LIC_STATE_ID").cast("int").isNull()
        )

        speeding_related_offenses_df = self.Charges_use_df.filter(
            col("CHARGE").like("%SPEED%")
        )

        # Drivers with license
        drivers_with_license_df = self.Primary_person_use_df.filter(
            col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])
        )  # drivers_with_license_df.show(10)

        # uses top 10 used vehicle colours and has car licensed
        # with the Top 25 states with highest number of offences

        # Top 25 states
        top_25_states = vehicle_state_df.collect()
        top_25_states_list = [state_info[0] for state_info in top_25_states]
        # print(top_25_states_list)

        # Top 10 colors
        top_10_colors = top_ten_vehicle_colors.collect()
        top_10_colors_list = [color_info[0] for color_info in top_10_colors]
        # print(top_10_colors_list)

        # Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        # has licensed Drivers, uses top 10 used vehicle colours and
        # has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

        final_df_with_vehicle_make_info_df = (
            drivers_with_license_df.join(speeding_related_offenses_df, on=["CRASH_ID"])
            .join(self.Units_use_df, on=["CRASH_ID"])
            .filter(self.Units_use_df["VEH_COLOR_ID"].isin(top_10_colors_list))
            .filter(self.Units_use_df["VEH_LIC_STATE_ID"].isin(top_25_states_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        # Load output to CSV output in parquet format
        load_data_to_csv(final_df_with_vehicle_make_info_df, output_path)
        return final_df_with_vehicle_make_info_df.limit(5).collect()

        final_df_with_vehicle_make_info_df = final_df_with_vehicle_make_info_df.limit(5)
        final_df_with_vehicle_make_info_df.show()


if __name__ == "__main__":
    # Get configurations from config file 'config.yaml'
    path_to_config_file = "config.yaml"

    output_file_paths = read_config(path_to_config_file).get("OUTPUT")
    logging_config = read_config(path_to_config_file).get("LOGGING")

    # Initialize logger
    logger = logging.getLogger(logging_config.get("namespace"))
    logger.setLevel(logging_config.get("level"))

    formatter = logging.Formatter(logging_config.get("formatter"))

    file_handler = logging.FileHandler(
        logging_config.get("path"), mode=logging_config.get("mode")
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    # Initialize SparkSession - app name CarCrashAnalysis
    logger.info("Initializing SparkSession...")
    spark = SparkSession.builder.appName("CarCrashAnalysis").getOrCreate()
    logger.info(f"SparkSession now available as 'spark'. {spark._sc}\n")

    spark.sparkContext.setLogLevel("ERROR")
    # Create an object from CarCrashAnalysis Class
    car_crash_analysis = CarCrashAnalysis(path_to_config_file)

    # Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
    # According to data dictionary - Primary_Person_use.csv has the Car crash info with gender details - PRSN_GNDR_ID
    logger.info(
        "ANALYSIS 1: Number of crashes in which person killed is Male: "
        f"{car_crash_analysis.male_car_crash_analysis(output_file_paths.get('analysis_1_output'))}\n"
    )

    # Analysis 2: How many two wheelers are booked for crashes?
    # According to data dictionary - Units_use.csv has the vehicle type
    # info - VEH_BODY_STYL_ID
    logger.info(
        "ANALYSIS 2: Number of two wheelers booked for crashes: "
        f"{car_crash_analysis.two_wheeler_crash_analysis(output_file_paths.get('analysis_2_output'))}\n"
    )

    # Analysis 3: Which state has the highest number of accidents in which
    # females are involved?
    # According to data dictionary - Primary_Person_use.csv has the Car
    # crash info with gender and state details - PRSN_GNDR_ID, DRVR_LIC_STATE_ID

    # We already have the DF available as Primary_Person_use_df
    logger.info(
        "ANALYSIS 3: State with highest number of accidents "
        "involving females: "
        f"{car_crash_analysis.female_car_crash_analysis(output_file_paths.get('analysis_3_output'))}\n"
    )

    # Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute
    # to a largest number of injuries including death

    # According to data dictionary - Units_use.csv has the Car crash info
    # with injury count, death and VEH_MAKE_ID info

    # We already have the DF available as Units_use_df
    logger.info(
        "ANALYSIS 4: Top5th to 15th Vehicle make involved in accidents"
        " including death: \n"
        f"{car_crash_analysis.vehicle_make_crash_analysis(output_file_paths.get('analysis_4_output'))}\n"
    )

    # Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body
    # style

    # We have the vehicle body style info in Units_use.csv and ethnicity info in Primary_person_use.csv,
    # we already have this info in Units_use_df and Primary_person_use_df

    vehicle_and_ethnicity_list = car_crash_analysis.vehicle_body_style_crash_analysis(
        output_file_paths.get("analysis_5_output")
    )
    logger.info("ANALYSIS 5: Top ethnic user group for each vehicle body style: \n")
    for vehicle_info in vehicle_and_ethnicity_list:
        logger.info(f"{vehicle_info[0]}:  {vehicle_info[1]}")
    logger.info("\n")

    # Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as
    # the contributing factor to a crash (Use Driver Zip Code)

    # According to data dictionary - Primary_person_use.csv has the Car crash info with driver zip code and
    # alcohol result - DRVR_ZIP and PRSN_ALC_RSLT_ID, contributing factors are available in Units_use.csv  #### We
    # already have the DFs available as Primary_person_use_df and Units_use_df
    crashes_due_to_alcohol_list = car_crash_analysis.alcohol_related_crash_analysis(
        output_file_paths.get("analysis_6_output")
    )
    logger.info(
        "ANALYSIS 6: Top 5 Zip Codes with highest number crashes with alcohols "
        "as the contributing factor to a crash: \n"
    )
    for crash_info in crashes_due_to_alcohol_list:
        logger.info(f"{crash_info[0]}:  {crash_info[1]}")
    logger.info("\n")

    # Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~)
    # is above 4 and car avails Insurance

    # According to data dictionary - Damages_use.csv has the Car crash info with damages, Units_use.csv has info
    # regarding Damage level - VEH_DMAG_SCL, Insurance  #### We already have the DFs available as
    # Primary_person_use_df and Units_use_df

    logger.info(
        f"ANALYSIS 7: Count of Distinct Crash IDs where No Damaged "
        f"Property was observed and Damage Level (VEH_DMAG_SCL~) "
        "is above 4 and car avails Insurance: "
        f"{car_crash_analysis.insurance_related_crash_analysis(output_file_paths.get('analysis_7_output'))}\n"
    )

    # Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
    # has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest
    # number of offences (to be deduced from the data)

    # According to data dictionary, we have the Vehicle info in Units_use.csv, Driver details in
    # primary_peron_use.cs  #### We already have the DFs available as Primary_person_use_df and Units_use_df
    speeding_list = car_crash_analysis.speeding_related_crash_analysis(
        output_file_paths.get("analysis_8_output")
    )
    logger.info(
        f"ANALYSIS 8: top 5 Vehicle Makes where drivers are charged "
        f"with speeding related offences, has licensed Drivers, "
        f"uses top 10 used vehicle colours and has car licensed with "
        f"the Top 25 states with highest number of offences: "
    )
    for crash_info in speeding_list:
        logger.info(f"{crash_info[0]}:  {crash_info[1]}")
