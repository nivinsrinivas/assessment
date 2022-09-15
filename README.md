# Car Crash Analysis

This Readme explains the instructions to setup the pyspark environment and run the spark job.

## Project Structure

```bash
assessment [referred below as root too]
|-- Pipfile
|-- Pipfile.lock
|-- README.md
|-- analysis.ipynb
|-- analysis.py
|-- build_dependencies.sh
|-- config.yaml
|-- input
|    -- Data
|       |-- Charges_use.csv
|       |-- Damages_use.csv
|       |-- Endorse_use.csv
|       |-- Primary_Person_use.csv
|       |-- Restrict_use.csv
|       |-- Units_use.csv
|-- run.sh
|-- utils
    |-- helper.py
```

## Managing Project Dependencies using Pipenv
### Note: Python3 must be installed as a prerequisite
    Python Version used here: 3.10


We use [pipenv](https://docs.pipenv.org) for managing project dependencies and Python environments (i.e. virtual environments). All direct packages dependencies (e.g. NumPy may be used in a User Defined Function), as well as all the packages used during development (e.g. PySpark, flake8 for code linting, IPython for interactive console sessions, etc.), are described in the `Pipfile`. Their **precise** downstream dependencies are described in `Pipfile.lock`.

### Installing Pipenv

To get started with Pipenv, first of all download it - assuming that there is a global version of Python available on your system and on the PATH, then this can be achieved by running the following command,

```bash
pip3 install pipenv
```

Pipenv is also available to install from many non-Python package managers. For example, on OS X it can be installed using the [Homebrew](https://brew.sh) package manager, with the following terminal command,

```bash
brew install pipenv
```

For more information, including advanced configuration options, see the [official pipenv documentation](https://docs.pipenv.org).


## Important scripts 

#### build_dependencies.sh script
* Builds a pipenv with all dependencies and packages required for the analysis.
* It zips the packages into packages.zip in root directory.
* This package.zip is passed as a parameter in --py-files while running spark-submit.

#### run.sh script 
* It is the main entrypoint to the entire Spark Analysis.
* Runs the build_dependencies.sh script.
  * Set up pipenv with PySpark and other dependencie.s
* Run Black formatting on analysis.py.
* Execute spark-submit with the built packages.

## Running the analysis
#### Clone this assessment repo
```git clone <repo_name>```
#### Run the command:
```cd assessment && chmod u+x run.sh && ./run.sh```

### Switch Python Version
#### In case you don't have Python3.10, follow these instructions
* Open Pipfile
* Change Python Version in ```[requires] ``` section
to Python Version you have [>=3.6] 
* set ```python_version = "3.6"``` [for example]
* Save the file and run ```pipenv --python 3.6```
* Head Over to **Running the analysis** section and follow the instructions.

## The Input
* Input CSV files are available in input/*.csv 

## The Output
* The Output [Single line solutions] of the Analysis is logged in a file called `car_crash_analysis.log` in root directory.
* The Dataframe output is written in output directory in individual subfolders for each analysis problem in parquet format.


## Project structure after spark-submit
```bash
assessment
|-- Pipfile
|-- Pipfile.lock
|-- README.md
|-- analysis.ipynb
|-- analysis.py
|-- build_dependencies.sh
|-- car_crash_analysis.log
|-- config.yaml
|-- input
|   |-- Data
|       |-- Charges_use.csv
|       |-- Damages_use.csv
|       |-- Endorse_use.csv
|       |-- Primary_Person_use.csv
|       |-- Restrict_use.csv
|       |-- Units_use.csv
|-- output
|   |-- analysis_1
|   |   |-- _SUCCESS
|   |   |-- part-00000-72b14d78-1f5b-433b-94e7-6c4f799c13ae-c000.snappy.parquet
|   |-- analysis_2
|   |   |-- _SUCCESS
|   |   |-- part-00000-7a5bc396-a8c0-469a-bbc6-3e88e0d67ece-c000.snappy.parquet
|   |-- analysis_3
|   |   |-- _SUCCESS
|   |   |-- part-00000-3c5da559-2239-4d0f-87d8-7b94e3068e3a-c000.snappy.parquet
|   |-- analysis_4
|   |   |-- _SUCCESS
|   |   |-- part-00000-c10c9ec2-182d-40a9-8612-1cbcb2de190f-c000.snappy.parquet
|   |-- analysis_5
|   |   |-- _SUCCESS
|   |   |-- part-00000-82f19e19-6038-4b91-af8f-8f655a30a041-c000.snappy.parquet
|   |-- analysis_6
|   |   |-- _SUCCESS
|   |   |-- part-00000-c5f43558-0ecd-4e77-96fc-1f431c33afac-c000.snappy.parquet
|   |-- analysis_7
|   |   |-- _SUCCESS
|   |   |-- part-00000-de550197-130a-4a03-a35e-cd43e6b0c67e-c000.snappy.parquet
|   |-- analysis_8
|       |-- _SUCCESS
|       |-- part-00000-74e8a0a0-db5b-4e9d-a36a-7b581080f9c4-c000.snappy.parquet
|-- packages.zip
|-- run.sh
|-- utils
    |-- helper.py
```

#### The output directory is now available with parquet files for each analysis
#### Solutions are logged in `car_crash_analysis.log`