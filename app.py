from flask import Flask, g, jsonify, make_response
from flask_restplus import Api, Resource, fields
import sqlite3
from os import path


DB = 'NSW_Suburb_Crime_Data_Jan_95_Dec_18.sqlite'
TABLE_NAME = 'NSW_Suburb_Crime_Data_Jan_95_Dec_18'
TITLE = 'Data Service for NSW monthly suburb crime figures from Janurary 1995 to December 2018.'
DESCRIPTION = 'This is a Flask-Restplus data service that allows a client to consume APIs related to NSW monthly suburb crime figures from Janurary 1995 to December 2018.\n Relatives Postcode(s) were obtained from AustPost\'s "standard_postcode_file_pc001_16062019" and https://postcodes-australia.com/state-postcodes/nsw, postcodes may be missing from certain suburbs.'

# All data.
MSG_ALL = 'Retrieving all records from the database for all suburbs.'
COLUMNS = ['SUBURB', 'RELATIVE_POSTCODE(S)', 'OFFENCE_CATEGORY', 'SUBCATEGORY', 'PERIOD', 'COUNT']

# SUBURBS
MSG_SUB = 'Retrieving all records from the database for the specified suburb.'

# RELATIVE_POSTCODE
MSG_POSTCODE = 'Retrieving all records from the database for the specified postcode.'

# OFFENCE_CATEGORY
MSG_OFF_CAT = 'Retrieving all records from the database for the specified offence category.'

# SUBCATEGORY
MSG_SUB_CAT = 'Retrieving all records from the database for the specified subcategory.'

# PERIOD
MSG_PERIOD = 'Retrieving all records from the database for the specified period.'

app = Flask(__name__)
api = Api(app, version='1.0', title=TITLE,
          description=DESCRIPTION,
          )

# Database helper
ROOT = path.dirname(path.realpath(__file__))


def connect_db():
    sql = sqlite3.connect(path.join(ROOT, DB))
    sql.row_factory = sqlite3.Row
    return sql


def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@api.route('/all')
class NSWCrimeAll(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description=MSG_ALL)
    def get(self):
        db = get_db()
        details_cur = db.execute(
            'select * from ' + TABLE_NAME)
        details = details_cur.fetchall()
        return_values = []

        for detail in details:
            detail_dict = {}
            for column in COLUMNS:
                detail_dict[column] = detail[column]

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/SUBURB/<string:SUBURB>')
class NSWCrimeSuburb(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description=MSG_SUB)
    def get(self, SUBURB):
        db = get_db()
        details_cur = db.execute(
            'select * from ' + TABLE_NAME + ' where SUBURB like ? COLLATE NOCASE', [SUBURB])
        details = details_cur.fetchall()
        return_values = []

        for detail in details:
            detail_dict = {}
            for column in COLUMNS:
                detail_dict[column] = detail[column]

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/RELATIVE_POSTCODE(S)/<int:RELATIVE_POSTCODE>')
class NSWCrimePostcode(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description=MSG_POSTCODE)
    def get(self, RELATIVE_POSTCODE):
        db = get_db()
        details_cur = db.execute(
            'select * from ' + TABLE_NAME + ' where "RELATIVE_POSTCODE(S)" like ? COLLATE NOCASE', ['%' + str(RELATIVE_POSTCODE) + '%'])
        details = details_cur.fetchall()
        return_values = []

        for detail in details:
            detail_dict = {}
            for column in COLUMNS:
                detail_dict[column] = detail[column]

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/OFFENCE_CATEGORY/<string:OFFENCE_CATEGORY>')
class NSWCrimeOffCat(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description=MSG_OFF_CAT)
    def get(self, OFFENCE_CATEGORY):
        db = get_db()
        details_cur = db.execute(
            'select * from ' + TABLE_NAME + ' where OFFENCE_CATEGORY like ? COLLATE NOCASE', [OFFENCE_CATEGORY])
        details = details_cur.fetchall()
        return_values = []

        for detail in details:
            detail_dict = {}
            for column in COLUMNS:
                detail_dict[column] = detail[column]

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/SUBCATEGORY/<string:SUBCATEGORY>')
class NSWCrimeSubCat(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description=MSG_SUB_CAT)
    def get(self, SUBCATEGORY):
        db = get_db()
        details_cur = db.execute(
            'select * from ' + TABLE_NAME + ' where SUBCATEGORY like ? COLLATE NOCASE', [SUBCATEGORY])
        details = details_cur.fetchall()
        return_values = []

        for detail in details:
            detail_dict = {}
            for column in COLUMNS:
                detail_dict[column] = detail[column]

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/PERIOD/<string:PERIOD>')
class NSWCrimePeriod(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description=MSG_PERIOD)
    def get(self, PERIOD):
        db = get_db()
        details_cur = db.execute(
            'select * from ' + TABLE_NAME + ' where PERIOD like ? COLLATE NOCASE', [PERIOD])
        details = details_cur.fetchall()
        return_values = []

        for detail in details:
            detail_dict = {}
            for column in COLUMNS:
                detail_dict[column] = detail[column]

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


if __name__ == '__main__':
    app.run()
