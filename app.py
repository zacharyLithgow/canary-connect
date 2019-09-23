import json
import sqlite3
import time

from flask import Flask, request
from flask.json import jsonify

from utils import median

app = Flask(__name__)

# Setup the SQLite DB
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()


@app.route('/devices/<string:device_uuid>/readings/', methods=['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if request.method == 'POST':
        # Grab the post parameters
        if request.data:
            post_data = json.loads(request.data)
        else:
            return 'missing data in the request parameters', 400

        sensor_type = post_data.get('type', None)
        value = post_data.get('value', None)
        date_created = post_data.get('date_created', int(time.time()))

        if not sensor_type or sensor_type not in ('temperature', 'humidity'):
            return 'the sensor type is not valid', 400

        if not value or 100 < value or 0 > value:
            return 'the sensor value is not in the mandatory range of 0-100', 400

        # Insert data into db
        cur.execute('INSERT INTO readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))

        conn.commit()

        # Return success
        return 'success', 201
    else:
        # Grab the query parameters (if any)
        start = end = type = None
        if request.data:
            post_data = json.loads(request.data)
            start = post_data.get('start', None)
            end = post_data.get('end', None)
            type = post_data.get('type', None)

        sql = 'SELECT * from readings WHERE device_uuid = ?'
        params = [device_uuid]
        if start:
            sql += 'AND date_created >= ?'
            params += [start]
        if end:
            sql += 'AND date_created <= ?'
            params += [end]
        if type:
            sql += 'AND type = ?'
            params += [type]

        # Execute the query
        cur.execute(sql, params)
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


@app.route('/devices/<string:device_uuid>/readings/min/', methods=['GET'])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    if request.data:
        post_data = json.loads(request.data)
        type = post_data.get('type', None)
        if not type or type not in ('temperature', 'humidity'):
            return 'error on the required type data', 400
        start = post_data.get('start', None)
        end = post_data.get('end', None)
    else:
        return 'missing data in the request parameters', 400

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = 'SELECT *, MIN(r.value) from readings r WHERE r.type = ? AND r.device_uuid = ?'
    params = [type, device_uuid]
    if start:
        sql += 'AND r.date_created >= ?'
        params += [start]
    if end:
        sql += 'AND r.date_created <= ?'
        params += [end]

    # Execute the query
    cur.execute(sql, params)
    row = cur.fetchone()

    if not row[0]:
        return 'No results found', 200

    # Return the JSON
    return jsonify(dict(zip(['device_uuid', 'type', 'value', 'date_created'], row))), 200


@app.route('/devices/<string:device_uuid>/readings/max/', methods=['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    if request.data:
        post_data = json.loads(request.data)
        type = post_data.get('type', None)
        if not type or type not in ('temperature', 'humidity'):
            return 'error on the required type data', 400
        start = post_data.get('start', None)
        end = post_data.get('end', None)
    else:
        return 'missing data in the request parameters', 400

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = 'SELECT *, MAX(r.value) from readings r WHERE r.type = ? AND r.device_uuid = ?'
    params = [type, device_uuid]
    if start:
        sql += 'AND r.date_created >= ?'
        params += [start]
    if end:
        sql += 'AND r.date_created <= ?'
        params += [end]

    # Execute the query
    cur.execute(sql, params)
    row = cur.fetchone()

    if not row[0]:
        return 'No results found', 200

    # Return the JSON
    return jsonify(dict(zip(['device_uuid', 'type', 'value', 'date_created'], row))), 200


@app.route('/devices/<string:device_uuid>/readings/median/', methods=['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    if request.data:
        post_data = json.loads(request.data)
        type = post_data.get('type', None)
        if not type or type not in ('temperature', 'humidity'):
            return 'error on the required type data', 400
        start = post_data.get('start', None)
        end = post_data.get('end', None)
    else:
        return 'missing data in the request parameters', 400

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = 'SELECT r.value from readings r WHERE r.type = ? AND r.device_uuid = ?'
    params = [type, device_uuid]
    if start:
        sql += 'AND r.date_created >= ?'
        params += [start]
    if end:
        sql += 'AND r.date_created <= ?'
        params += [end]

    sql += 'ORDER BY r.value'

    # Execute the query
    cur.execute(sql, params)
    rows = [row[0] for row in cur.fetchall()]

    if len(rows) == 0:
        return 'No results found', 200

    return str(median(rows)), 200


@app.route('/devices/<string:device_uuid>/readings/mean/', methods=['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    if request.data:
        post_data = json.loads(request.data)
        type = post_data.get('type', None)
        if not type or type not in ('temperature', 'humidity'):
            return 'error on the required type data', 400
        start = post_data.get('start', None)
        end = post_data.get('end', None)
    else:
        return 'missing data in the request parameters', 400

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = 'SELECT AVG(r.value) from readings r WHERE r.type = ? AND r.device_uuid = ?'
    params = [type, device_uuid]
    if start:
        sql += 'AND r.date_created >= ?'
        params += [start]
    if end:
        sql += 'AND r.date_created <= ?'
        params += [end]

    # Execute the query
    cur.execute(sql, params)
    row = cur.fetchone()

    if not row[0]:
        return 'No results found', 200

    return str(row[0]), 200


@app.route('/devices/<string:device_uuid>/readings/mode/', methods=['GET'])
def request_device_readings_mode(device_uuid):
    """
    This endpoint allows clients to GET the mode sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    if request.data:
        post_data = json.loads(request.data)
        type = post_data.get('type', None)
        if not type or type not in ('temperature', 'humidity'):
            return 'error on the required type data', 400
        start = post_data.get('start', None)
        end = post_data.get('end', None)
    else:
        return 'missing data in the request parameters', 400

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = 'SELECT r.value from readings r WHERE r.type = ? AND r.device_uuid = ?'
    params = [type, device_uuid]
    if start:
        sql += 'AND r.date_created >= ?'
        params += [start]
    if end:
        sql += 'AND r.date_created <= ?'
        params += [end]

    sql += 'GROUP BY r.value ORDER BY COUNT(*) DESC LIMIT 1'

    # Execute the query
    cur.execute(sql, params)
    row = cur.fetchone()

    if not row[0]:
        return 'No results found', 200

    return str(row[0]), 200


@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods=['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    if request.data:
        post_data = json.loads(request.data)
        type = post_data.get('type', None)
        if not type or type not in ('temperature', 'humidity'):
            return 'error on the required type data', 400
        start = post_data.get('start', None)
        if not start:
            return 'error on the required start data', 400
        end = post_data.get('end', None)
        if not end:
            return 'error on the required end data', 400
    else:
        return 'missing data in the request parameters', 400

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = 'SELECT r.value from readings r WHERE r.type = ? AND r.device_uuid = ? AND r.date_created >= ? AND r.date_created <= ?'
    params = [type, device_uuid, start, end]

    sql += 'ORDER BY r.value'

    # Execute the query
    cur.execute(sql, params)
    rows = [row[0] for row in cur.fetchall()]

    mid = len(rows) // 2

    if (len(rows) % 2 == 0):
        # even
        lowerQ = median(rows[:mid])
        upperQ = median(rows[mid:])
    else:
        # odd
        lowerQ = median(rows[:mid])  # same as even
        upperQ = median(rows[mid + 1:])

    return str(lowerQ) + "," + str(upperQ), 200


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
