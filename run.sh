# Execution Start Time
start=$(date +%s)
# Build dependencies
echo "=============================================================="
echo "Building project dependencies"
chmod u+x build_dependencies.sh
./build_dependencies.sh

# Run black formatting
echo "=============================================================="
echo "Running black formatting on analysis.py"
pipenv run black analysis.py

# Run the spark job using spark-submit within the pipenv
echo "=============================================================="
echo "Running the spark-submit..."
pipenv run spark-submit --master local[*] --py-files packages.zip analysis.py

echo "=============================================================="
echo "Successfully completed the Car Crash Analysis."
echo "=============================================================="
echo "Please check car_crash_analysis.log in root directory for the solutions and out Dataframes written in newly created output directory."
echo "=============================================================="
# Time elapsed
echo "Elapsed Time: $(($end-$start)) seconds."
echo "===========================DONE!============================"