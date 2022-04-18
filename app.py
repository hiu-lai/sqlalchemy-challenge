from asyncio.proactor_events import _ProactorSocketTransport
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measurement = Base.classes.measurement
station =  Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start_date&gt;<br/>"
        f"/api/v1.0/&lt;start_date&gt;/&lt;end_date&gt;<br/>"
        f"<br/>"
        f"<b>NB: </b><i>&lt;start_date&gt;</i> and <i>&lt;end_date&gt;</i> format YYYY-MM-DD ie 2022-04-19"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all precipitation by date"""
    # Query all prcp
    results = session.query(measurement.station, measurement.date, measurement.prcp).all()

    session.close()

    # Convert list of tuples into normal list
    # all_prcp = list(np.ravel(results))
    all_measurement = []
    for station, date, prcp in results:
        measurement_dict = {}
        measurement_dict["station"] = station
        measurement_dict["date"] = date
        measurement_dict["prcp"] = prcp
        all_measurement.append(measurement_dict)

    return jsonify(all_measurement)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    results = session.query(station.station, station.name, station.latitude, station.longitude, station.elevation).all()

    session.close()

    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query the dates and temperature observations of the most active station for the last year of data.
    # Return a JSON list of temperature observations (TOBS) for the previous year.

    # Identify most active station and most recent date
    sel = [measurement.station, func.count(measurement.station), func.max(measurement.date)]

    active_station = session.query(*sel).group_by(measurement.station).order_by(func.count(measurement.station).desc()).all()[0][0]
    max_date = session.query(*sel).group_by(measurement.station).order_by(func.max(measurement.date).desc()).all()[0][2]
    LYDate=(datetime.strptime(max_date, "%Y-%m-%d")  - relativedelta(years=1)).date()

    # Query all Measurements with filter
    results = session.query(measurement.station, measurement.date, measurement.tobs).filter(measurement.date > LYDate).filter(measurement.station == active_station).all()

    session.close()

    all_tobs = []
    for station, date, tobs in results:
        tobs_dict = {}
        tobs_dict["station"] = station
        tobs_dict["date"] = date
        tobs_dict["tobs"] = tobs
        all_tobs.append(tobs_dict)

    return jsonify(all_tobs)

@app.route("/api/v1.0/<start>")

def start_date(start):
    # """Fetch the Justice League character whose superhero matches
    #    the path variable supplied by the user, or a 404 if not."""
    session = Session(engine)
    date_list =[]

    date = session.query(measurement.date, func.min(measurement.prcp), func.max(measurement.prcp), func.avg(measurement.prcp)).\
            group_by(measurement.date).order_by(measurement.date.asc()).all()
    keys = len(date)
    for x in range(keys):
        date_dicts = {}
        date_dicts['date'] = date[x][0] 
        date_dicts['TMIN:'] = date[x][1]
        date_dicts['TAVG:'] = date[x][2]
        date_dicts['TMAX:'] = date[x][1]
        date_list.append(date_dicts)
    date_list

    all_date = []

    canonicalized = start
    for s_date in date_list:
        x = s_date['date']
        if x >= canonicalized:
            all_date.append(s_date)
    return jsonify(all_date)

    # return jsonify({"error": "date not found."}), 404

@app.route("/api/v1.0/<start>/<end>")

def date_param(start, end):
    # """Fetch the Justice League character whose superhero matches
    #    the path variable supplied by the user, or a 404 if not."""
    session = Session(engine)
    date_list =[]

    date = session.query(measurement.date, func.min(measurement.prcp), func.max(measurement.prcp), func.avg(measurement.prcp)).\
            group_by(measurement.date).order_by(func.count(measurement.date).desc()).all()
    keys = len(date)
    for x in range(keys):
        date_dicts = {}
        date_dicts['date'] = date[x][0] 
        date_dicts['TMIN:'] = date[x][1]
        date_dicts['TAVG:'] = date[x][2]
        date_dicts['TMAX:'] = date[x][1]
        date_list.append(date_dicts)
    date_list

    all_date = []

    canonicalized = start
    for s_date in date_list:
        x = s_date['date']
        if (x >= canonicalized and x <= end):
            all_date.append(s_date)
    return jsonify(all_date)
#     return jsonify({"error": "date not found."}), 404

if __name__ == '__main__':
    app.run(debug=True)
