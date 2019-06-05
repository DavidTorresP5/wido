# wido

:snake: python3 :snake:

A data pipeline application where you can upload data files, validate them and convert to a common format.

- Validate inputs (quantity, file sizes, extension...)
- Multiple input formats (extensible) → transformation to csv (TODO: Apache Arrow) and save them with an unique identifier
- UI Output 1: dataframe with sampling.
- UI Output 2: data quality and basic stats about dataframe columns.
- Utils: logging, timer, slack connector for notifications, TODO: AWS s3... )


## Use

1. Clone the repo
2. Create a virtual enviroment and install requeriments

~~~~
python -m venv /path/to/virtual/env
pip install -r requirements.txt
~~~~

3. Launch the app

~~~~
python app.py
~~~~
