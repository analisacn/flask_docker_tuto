import json
import pandas as pd
import os

from flask import Flask
from flask_restplus import Api, Resource, abort, reqparse
from werkzeug.datastructures import FileStorage

from src.db import init_db, connect, close_connection

UPLOAD_DIRECTORY = os.getenv("DOCS_PATH", "docs/")
WEB_PORT = os.getenv("WEB_PORT", 5001)

app = Flask(__name__)
api = Api(app)

upload_parser = reqparse.RequestParser()
upload_parser.add_argument('file', location='files', type=FileStorage,
                           required=True)

measurement_parser = reqparse.RequestParser()
measurement_parser.add_argument('from_date', type=str, required=True)
measurement_parser.add_argument('to_date', type=str, required=True)
measurement_parser.add_argument('variable', type=str, required=True)
measurement_parser.add_argument('statistic_m', type=str, required=True)


@api.route('/upload/')
class FileUpload(Resource):
    def save_file(self, args):
        """
        Save file into the container and create register into the DB
        :param args: uploaded_file with file
        """
        uploaded_file = args['file']
        destination = os.path.join(UPLOAD_DIRECTORY)
        if not os.path.exists(destination):
            abort(400, "Destination folder does not exist.")
        file_path = os.path.join(destination, uploaded_file.filename)
        uploaded_file.save(file_path)

        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute(
            "INSERT INTO files (filename) "
            "VALUES (%s);", (uploaded_file.filename,))
        db_conn.commit()
        close_connection(db_conn, cursor=cursor)

    @api.expect(upload_parser)
    def post(self):
        """
        Upload a new File
        """
        args_ = upload_parser.parse_args()
        self.save_file(args_)
        return (200, "File successfully uploaded")


@api.route('/files/')
class File(Resource):
    def get(self):
        """
        Get all files name to know wich one will be processed
        :return:
        """
        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute("SELECT * FROM files")
        files = cursor.fetchall()
        close_connection(db_conn, cursor=cursor)
        return files


@api.route('/files/<int:file_id>/process/')
class ProcessFile(Resource):

    def post(self, file_id):
        """
        Process file by filename
        :return:
        """
        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT * FROM files WHERE id = {file_id}")
        file = cursor.fetchone()

        if not file:
            abort(404, "File cannot be found.")

        filename = file[1]
        filepath = os.path.join(UPLOAD_DIRECTORY, filename)

        if not os.path.exists(filepath):
            abort(400, "File cannot be found in directory.")

        df = pd.read_csv(filepath, dtype='str')

        if df.empty:
            return 200, "Empty file"

        json_lines = df.assign(index=df.index).to_dict(orient="records")

        for line in json_lines:
            # save row as json data into the table
            data = json.dumps(line)
            cursor.execute("INSERT INTO measurements (data) VALUES (%s)",
                           (data,))
        db_conn.commit()
        close_connection(db_conn, cursor=cursor)

        return 200


@api.route('/measurements/')
@api.expect(measurement_parser)
class Measurement(Resource):
    def get(self):
        """
            Devolver un JSON con los valores por estación de una medida
            estadística en un periodo temporal dado
        """

        statistic_m = {
            'avg': "SELECT AVG((data->>%s)::FLOAT) "
                   "FROM measurements "
                   "WHERE (data->>'created_at')::TIMESTAMP >= %s::TIMESTAMP "
                   "AND (data->>'created_at')::TIMESTAMP <= %s::TIMESTAMP",
            'sum': "SELECT SUM((data->>%s)::FLOAT) "
                   "FROM measurements "
                   "WHERE (data->>'created_at')::TIMESTAMP >= %s::TIMESTAMP "
                   "AND (data->>'created_at')::TIMESTAMP <= %s::TIMESTAMP",
            'max': "SELECT MAX((data->>%s)::FLOAT) "
                   "FROM measurements "
                   "WHERE (data->>'created_at')::TIMESTAMP >= %s::TIMESTAMP "
                   "AND (data->>'created_at')::TIMESTAMP <= %s::TIMESTAMP",
            'min': "SELECT MIN((data->>%s)::FLOAT) "
                   "FROM measurements "
                   "WHERE (data->>'created_at')::TIMESTAMP >= %s::TIMESTAMP "
                   "AND (data->>'created_at')::TIMESTAMP <= %s::TIMESTAMP",

        }
        args_ = measurement_parser.parse_args()
        query = statistic_m.get(args_['statistic_m'])

        if not query:
            return abort(400, "Unknown statistic_m, "
                              "expected one of: max, min, sum, avg")

        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute(query, (
        args_['variable'], args_['from_date'], args_['to_date']))
        result = cursor.fetchone()

        return {f'{args_["statistic_m"]}({args_["variable"]})': result[0]}


def main():
    init_db()
    app.run(port=WEB_PORT)


if __name__ == '__main__':
    main()
