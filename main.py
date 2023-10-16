import psycopg2

with open('parol_Postgres.txt', 'r') as file_object:
    parol_Postgres = file_object.read().strip()

def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE Python_Clients, Phones;
        """)
    pass

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Python_Clients(
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(40),
        surname VARCHAR(40),
        email VARCHAR(40),
        phones INTEGER
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Phones(
        phone_id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES Python_Clients(client_id),
        phone VARCHAR(15) UNIQUE
        );
        """)
    pass


def add_client(conn, name, surname, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO Python_Clients(name, surname, email, phones)
        VALUES (%s, %s, %s, %s) 
        RETURNING name, surname, email, phones;
        """, (name, surname, email, phones))
        print(cur.fetchone())
    pass


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO Phones(client_id, phone)
        VALUES (%s, %s)
        RETURNING client_id, phone;
        """, (client_id, phone))
        print(cur.fetchone())
    pass


def change_client(conn, client_id, name=None, surname=None, email=None, phone=None):
    if name is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE Python_Clients
            SET name = %s
            WHERE client_id = %s
            RETURNING client_id;        
            """, (name, client_id))
            cur.execute("""
            SELECT * FROM Python_Clients;
            """,)
            print('изменения:', cur.fetchall())
        pass
    if surname is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE Python_Clients
            SET surname = %s
            WHERE client_id = %s
            RETURNING client_id;        
            """, (surname, client_id))
            cur.execute("""
            SELECT * FROM Python_Clients;
            """,)
            print('изменения:', cur.fetchall())
        pass
    if email is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE Python_Clients
            SET email = %s
            WHERE client_id = %s
            RETURNING client_id;        
            """, (email, client_id))
            cur.execute("""
            SELECT * FROM Python_Clients;
            """,)
            print('изменения:', cur.fetchall())
        pass
    if phone is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE Phones
            SET phone = %s
            WHERE client_id = %s
            RETURNING client_id;
            """, (phone, client_id))
            cur.execute("""
            SELECT * FROM Phones;
            """)
            print('изменения:', cur.fetchall())
        pass

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM Phones
        WHERE client_id = %s AND phone = %s;
        """, (client_id, phone))
        cur.execute("""
        SELECT * FROM Phones;
        """)
        print('остались:', cur.fetchall())
    pass


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM Phones
        WHERE client_id = %s;
        """, (client_id,))
        cur.execute("""
        SELECT * FROM Phones;
        """)
        cur.execute("""
        DELETE FROM Python_Clients
        WHERE client_id = %s;
        """, (client_id,))
        cur.execute("""
        SELECT * FROM Python_Clients;
        """)
        print('остались:', cur.fetchall())
    pass


def find_client(conn, name=None, surname=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT pc.name, pc.surname, pc.email, pc.phones FROM Python_Clients pc
        JOIN phones p ON pc.client_id = p.client_id
        WHERE (pc.name = %s OR %s IS NULL)
            AND (pc.surname = %s OR %s IS NULL) 
            AND (pc.email = %s OR %s IS NULL) 
            AND (p.phone = %s OR %s IS NULL);
        """, (name, name, surname, surname, email, email, phone, phone))
        print(cur.fetchone())
    pass


with psycopg2.connect(database="python_clients_db", user="postgres", password=parol_Postgres) as conn:
    print('дропаем базу данных:')
    drop_db(conn)
    print('создаём базу данных:')
    create_db(conn)
    print('добавим Иванова без телефонов:')
    add_client(conn, "Ivan", "Ivanov", "Ivanov@mail.ru")
    print('добавим Петрова с одним телефоном:')
    add_client(conn, "Petr", "Petrov", "Petrov@mail.ru", 1)
    print('добавим Сидорова с двумя телефонами')
    add_client(conn, "Sidor", "Sidorov", "Sidorov@mail.ru", 2)
    print('добавим Кузнецова с тремя телефонами:')
    add_client(conn, "Kuzya", "Kuznectov", "Kuznetcov@mail.ru", 3)
    print('телефон Петрова:')
    add_phone(conn, 2, "+7(911)1111111")
    print('добавим 1-й телефон Сидорова:')
    add_phone(conn, 3, "+7(921)1111112")
    print('добавим 2-й телефон Сидорова:')
    add_phone(conn, 3, "+7(931)1111113")
    print('добавим 1-й телефон Кузнецова:')
    add_phone(conn, 4, "+7(941)1111114")
    print('добавим 2-й телефон Кузнецова:')
    add_phone(conn, 4, "+7(951)1111115")
    print('добавим 3-й телефон Кузнецова:')
    add_phone(conn, 4, "+7(961)1111116")
    print('изменим email и телефон Петрова:')
    change_client(conn, 2, None, None, "Petrov@gmail.com", "+200000")
    print('удалим 3-й телефон Кузнецова:')
    delete_phone(conn, 4, "+7(961)1111116")
    print('удалим Петрова:')
    delete_client(conn, 2)
    print('найдём Сидорова:')
    find_client(conn, None, None, "Sidorov@mail.ru", None)
    pass

conn.close()
