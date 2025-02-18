import mysql.connector
from mysql.connector import Error

class DB:
    def __init__(self):
        self.conn = None
        self.mycursor = None
        try:
            self.conn = mysql.connector.connect(
                host='127.0.0.1',
                user='root',
                password='rimjhim5s',
                database='Flights'
            )
            self.mycursor = self.conn.cursor()
            print('Connection Established')
        except Error as err:
            print(f'Connection Error: {err}')
        
    def fetch_city_names(self):
        if self.mycursor is None:
            print("No connection to the database.")
            return []
        try:
            city = []
            self.mycursor.execute("""
            SELECT DISTINCT Destination FROM flights
            UNION
            SELECT DISTINCT Source FROM flights
            """)
            data = self.mycursor.fetchall()
            for item in data:
                city.append(item[0])
            return city
        except Error as err:
            print(f'Error: {err}')
            return []
    
    def fetch_all_flights(self, source, destination):
        if self.mycursor is None:
            print("No connection to the database.")
            return []
        try:
            query = """
                SELECT Airline, Route, Dep_Time, Duration, Price 
                FROM flights 
                WHERE Source = %s AND Destination = %s
            """
            self.mycursor.execute(query, (source, destination))
            data = self.mycursor.fetchall()
            return data
        except Error as err:
            print(f'Error: {err}')
            return []
    
    def fetch_airline_frequency(self):
        airline = []
        frequency = []
        self.mycursor.execute("""
        SELECT Airline,COUNT(*) FROM flights
        GROUP BY Airline
        """)

        data = self.mycursor.fetchall()

        for item in data:
            airline.append(item[0])
            frequency.append(item[1])
        
        return airline,frequency
    
    def busy_airport(self):
        city = []
        frequency = []
        self.mycursor.execute("""
        SELECT Source,COUNT(*) FROM (SELECT Source FROM Flights.flights
                                     UNION ALL
                                     SELECT Destination FROM Flights.flights) AS T
        GROUP BY T.Source
        ORDER BY COUNT(*) DESC
        """)

        data = self.mycursor.fetchall()

        for item in data:
            city.append(item[0])
            frequency.append(item[1])
        
        return city,frequency
    
    def daily_frequency(self):
        date = []
        frequency = []
        self.mycursor.execute("""
        SELECT Date_of_Journey, COUNT(*) FROM Flights.flights
        GROUP BY Date_of_Journey
        """)

        data = self.mycursor.fetchall()

        for item in data:
            date.append(item[0])
            frequency.append(item[1])
        
        return date,frequency