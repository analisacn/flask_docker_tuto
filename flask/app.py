from flask import Flask
import psycopg2

app = Flask(__name__)


@app.route('/')
def hello_world():
    db_conn = connect()
    cur = db_conn.cursor()

    # Create a table
    cur.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        "id serial PRIMARY KEY, "
        "username text NOT NULL UNIQUE, "
        "first_name text NOT NULL, "
        "last_name text NOT NULL);"
    )
    # Save changes to te DB
    db_conn.commit()

    # Add a row
    add_user(cur, db_conn, 'analisac', 'Analisa', 'Carbone')

    # Let's get some data
    user = get_user_by_username(cur, "analisac")

    # Close database connection
    close_connection(db_conn, cur=cur)
    msg = user and f'Hello {user[2]} {user[3]}' or "Hello Guest"
    return msg


def add_user(cur, conn, username, first_name='', last_name=''):
    exists = get_user_by_username(cur, username)
    if not exists:
        cur.execute("INSERT INTO users (username, first_name, last_name) "
                    "VALUES (%s, %s, %s)", (username, first_name, last_name)
                    )
        conn.commit()


def get_user_by_username(cur, username):
    cur.execute(f"SELECT * FROM users WHERE username = '{username}'")
    user = cur.fetchone()  # (1, 'analisac', 'Analisa', 'Carbone')
    return user


def close_connection(connection, cur=False):
    if cur:
        cur.close()
    connection.close()


def connect():

    c = psycopg2.connect(host='postgresql', port=5432, user='postgres',
                          password='postgresexample', dbname='testdb')
    return c


if __name__ == '__main__':
    app.run()
