
## My Beam Pipeline Project

## Introduction
This project contains an Apache Beam pipeline for processing transaction data.


## Setting up the virtual environment
1. Ensure that Python version 3.11 and pip version 23.2.1 is installed on the system. 

2. Create a new virtual env specific to this project e.g. 
    ```
    python3.11 -m venv  .venv
    ```
    where `.venv` is the folder the virtual environment is configured in.  `.venv` is only an example, it can be a path as well.
    
3. Activate the virtual environment e.g.
    ```
    source .venv/bin/activate
    ```

Once the virtual environment is activated, the project Python libraries can be installed.

### Installing Python libraries

1.  Change to the root folder of this project e.g.
    ```
    $ cd ~/Data-Engineer-Tech-Test
    ```
2.  Install the project libraries using the `requirements.txt` file i.e.
    ```
    pip install -r requirements.txt
    ```
### Unit tests
    
1.  Change to the root folder of this project e.g.
    ```
    $ cd ~/Data-Engineer-Tech-Test
    ```
    
2.  Run the unit tests using this command `python -m pytest tests/`



## Usage
To run the pipeline, change to the root folder and 

1. `python src/task1.py`
2. `python src/task2.py`


## Project Structure
- `src/task1.py`: Script to run the Apache Beam pipeline for Task1 (individual transform steps).
- `src/task2.py`: Contains the composite transforms and other processing logic.
- `tests/`: Directory containing unit tests for the pipeline.
- `output/`: Directory where output files are stored. This directory is gitignored.

## Contributing
If you'd like to contribute to this project, please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/new-feature`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature/new-feature`).
6. Create a new Pull Request.

