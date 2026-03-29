# Glossary (Module 4: API and JSON Data)

## API (Application Programming Interface)

A web-accessible service that allows programs to request and receive data.
In this project, an API is used to retrieve structured text data in JSON format.

## Endpoint

A specific URL used to access data from an API.

## HTTP Request

A message sent from a client (your Python script) to a server (the API)
to request data.

## HTTP Request Headers

Metadata sent with an HTTP request that provides additional information
about the client or the requested data format.

## JSON (JavaScript Object Notation)

A structured data format commonly used by web APIs.
JSON data is organized using:

- objects (key-value pairs, like Python dictionaries)
- arrays (ordered lists, like Python lists)

## JSON Object

A collection of key-value pairs.
In Python, this is represented as a dictionary.

## JSON Array

An ordered collection of values.
In Python, this is represented as a list.

## Record

A single unit of data within a JSON structure.
In this project, each record typically represents one item (e.g., one post).

## Key

A name used to identify a value within a JSON object.

## Value

The data associated with a key in a JSON object.

## Nested Structure

A JSON structure that contains objects or arrays inside other objects or arrays.

## Pipeline

A sequence of processing stages where data flows from a source to a sink.

## EVTL (Extract, Validate, Transform, Load)

A pipeline model used in this project:

- **Extract**: acquire data from a source
- **Validate**: inspect structure and confirm the data is usable
- **Transform**: reshape the data into a structured format
- **Load**: write the data to a destination

## Source

The origin of data in a pipeline stage.
Examples include an API endpoint or an input file.

## Sink

The destination where data is written after processing.
Examples include a CSV file or database.

## Extract

The stage of the pipeline that retrieves data from an external source
and converts it into Python objects.

## Validate

The stage of the pipeline that inspects the structure of the data
and checks that it meets expectations before use.

## Transform

The stage of the pipeline that reshapes data into a structured,
analysis-ready format.

## Load

The stage of the pipeline that writes the processed data to a chosen destination.

## DataFrame

A tabular data structure with rows and columns.
In this project, Polars DataFrames are used to store structured data.

## Schema

The structure of data, including field names and expected data types.

## Inspection

The process of examining data to understand its structure,
including types, keys, and organization.

## Validation

The process of confirming that data meets expected structure,
types, and required fields.

## Normalization

The process of converting data into a consistent, structured format
where each record follows the same schema.

## Reproducibility

The ability to run the same pipeline and obtain consistent results,
given the same inputs and configuration.
