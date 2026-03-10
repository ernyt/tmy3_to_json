# tmy3_to_json
Python script that takes TMY3 data and station metadata and outputs a JSON file of weekly GHI &amp; DNI averages for each station

Files are downloaded from kaggle: https://www.kaggle.com/datasets/us-doe/tmy3-solar/data

Station metadata CSV file is included in the repo, but the TMY3 data file is too large and should be downloaded directly to your local computer.

To run this script, make sure the tmy3.csv and TMY3_StationsMeta.csv files are in the same
directory as this script. Use the following command to run the script
>  python tmy_to_json.py

This has been tested using versions:
python 3.9.0
pandas 2.2.0

Format of the outputted JSON files is as follows

[

{
"id": "the weather station ID or USAF", // string

"site_name": "the weather station site name", //string

"coordinates": [150, -26], // [float, float]

"data": [

{

"timestamp": 1724703772146, // integer

"ghi": 0, // integer

"dni": 0 // integer

}

]

}

]
