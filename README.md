# Car Crash Analysis

This Readme explains the instructions to setup the pyspark environment and run the spark job.

## Project Structure

```bash
assessment
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
* It zips the packages packages.zip in root directory.
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

## The Input
* Input CSV files are available in input/*.csv 

## The Output
* The Output [Single line solutions] of the Analysis is logged in a file called `car_crash_analysis.log` in root directory.
* The Dataframe output is written in output directory in individual subfolders for each analysis problem in parquet format.
