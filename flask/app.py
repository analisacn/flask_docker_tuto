import psycopg2
from flask import Flask
from flask_restplus import Api, Resource, abort, reqparse

app = Flask(__name__)
api = Api(app)

post_user_parser = reqparse.RequestParser(bundle_errors=True)
post_user_parser.add_argument('username', type=str, required=True)
post_user_parser.add_argument('first_name', type=str, required=True)
post_user_parser.add_argument('last_name', type=str, required=True)


def close_connection(connection, cursor=False):
    if cursor:
        cursor.close()
    connection.close()


def connect():
    c = psycopg2.connect(host='postgresql', port=5432, user='postgres',
                         password='postgresexample', dbname='testdb')
    return c


@api.route('/')
class HelloWorld(Resource):
    def create_table(self):
        db_conn = connect()
        cursor = db_conn.cursor()

        # Create a table
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "id serial PRIMARY KEY, "
            "username text NOT NULL UNIQUE, "
            "first_name text NOT NULL, "
            "last_name text NOT NULL);"
        )
        db_conn.commit()
        close_connection(db_conn, cursor=cursor)

    def get(self):
        self.create_table()
        msg = "Hello World"
        return {'msg': msg}


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
            abort(404, message=f"User {username} already exists")

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
        username = username.lower()
        db_conn = connect()
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT * FROM users WHERE username = '{username}'")
        user = cursor.fetchone()  # (1, 'analisac', 'Analisa', 'Carbone')
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


if __name__ == '__main__':
    app.run()
