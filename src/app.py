import os

from flask import Flask
from flask_restplus import Api, Resource, abort, reqparse
from werkzeug.datastructures import FileStorage

from .db import connect, close_connection, init_db

UPLOAD_DIRECTORY = os.getenv("DOCS_PATH", "docs/")

app = Flask(__name__)
api = Api(app)


post_user_parser = reqparse.RequestParser(bundle_errors=True)
post_user_parser.add_argument('username', type=str, required=True)
post_user_parser.add_argument('first_name', type=str, required=True)
post_user_parser.add_argument('last_name', type=str, required=True)


@api.route('/')
class HelloWorld(Resource):

    def get(self):
        return "Hello World"


@api.route('/users')
class Users(Resource):

    def get(self):
        """
        Get all users list
        """
        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT * FROM users")
        users = cursor.fetchall()
        close_connection(db_conn, cursor=cursor)
        return users

    @api.expect(post_user_parser)
    def post(self):
        """
        Add a new user
        """
        args = post_user_parser.parse_args()
        username = args['username'].lower()

        exists = User().get_user(username)

        if exists:
            abort(400, message=f"User {username} already exists")

        db_conn = connect()
        cursor = db_conn.cursor()

        cursor.execute(
            "INSERT INTO users (username, first_name, last_name) "
            "VALUES (%s, %s, %s)",
            (username, args['first_name'], args['last_name'])
        )
        db_conn.commit()
        close_connection(db_conn, cursor=cursor)
        user_created = User().get_user(username)
        return user_created, 201


@api.route('/user/<username>')
class User(Resource):

    def get_user(self, username):
        """
        Get user by username
        """
        username = username.lower()
        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT * FROM users WHERE username = '{username}'")
        user = cursor.fetchone()
        close_connection(db_conn, cursor=cursor)
        return user

    def get(self, username):
        user = self.get_user(username)
        if not user:
            abort(404, message=f"User {username} doesn't exist")

        user_dict = {
            'id': user[0],
            'username': user[1],
            'first_name': user[2],
            'last_name': user[3]
        }
        return user_dict, 200


upload_parser = reqparse.RequestParser()
upload_parser.add_argument('file', location='files', type=FileStorage,
                           required=True)


@api.route('/upload/')
class File(Resource):

    @api.expect(upload_parser)
    def post(self):
        """
        Upload a new File
        """
        args_ = upload_parser.parse_args()
        self.save_file(args_)
        return 200

    def save_file(self, args):
        uploaded_file = args['file']
        destination = os.path.join(UPLOAD_DIRECTORY)
        if not os.path.exists(destination):
            abort(400, "Destination folder does not exist.")
        file_path = os.path.join(destination, uploaded_file.filename)
        uploaded_file.save(file_path)


@api.route('/process/')
class ProcessFile(Resource):

    def post(self):
        return 200


if __name__ == '__main__':
    init_db()
    app.run()
