# Build dependencies
chmod u+x build_dependencies.sh
./build_dependencies.sh

# Run black formatting
black analysis.py

# Run the spark job using spark-submit within the pipenv
pipenv run spark-submit --master local[*] --py-files packages.zip analysis.py
