# Welcome to dataframe-generator

`dataframe-generator` is a simple Python module for generating test CSV datasets from PySpark schemas.

## Installation

`pip install dataframe-generator`

## Usage

The module has a simple CLI, which can be started with the following command:

`python -m dataframe_generator`

Note that only Python 3 is supported by this module.

As the first step you need to provide the schema of the dataset in the form of StructType definition, e.g.:

```
first_schema = StructType([
  StructField('name', StringType(), False),
  StructField('age', IntegerType(), False),
  StructField('birth_day', DateType(), True),
  StructField('address', StringType(), True)
])
```

After that just specify the number of desired rows and the potential preset values. See examples below.

## Supported data types

 * `ByteType()`
 * `ShortType()`
 * `IntegerType()`
 * `LongType()`
 * `DecimalType(x, y)`
 * `StringType()`
 * `DateType()`
 * `TimestampType()`


## Examples

### Generate test data from a single StructType
 <p align="center"><img src="docs/test_01.gif?raw=true"/></p>

### Generate test data with a simple preset value
Let's generate a dataset where the `age` field is fixed to 30.

<p align="center"><img src="docs/test_02.gif?raw=true"/></p>

### Generate test data with multiple preset values
Let's generate a dataset where the `name` field is fixed but the `age` can be
10, 20 or 30.

<p align="center"><img src="docs/test_03.gif?raw=true"/></p>

### Generate test data with multiple StructType definitions

<p align="center"><img src="docs/test_04.gif?raw=true"/></p>
