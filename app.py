import streamlit as st
import pandas as pd
import plotly.graph_objects
import plotly.express 

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

# Initialize the database connection
db = DB()

st.sidebar.title('Flights Analytics')

user_option = st.sidebar.selectbox('Menu', ['Select One', 'Check Flights', 'Analytics'])

if user_option == 'Check Flights':
    st.title('Check Flights')

    col1, col2 = st.columns(2)
    city = db.fetch_city_names()
    
    if city:
        with col1:
            source = st.selectbox('Source', city)
        with col2:
            destination = st.selectbox('Destination', city)
        
        if st.button('Search'):
            results = db.fetch_all_flights(source, destination)
            if results:
                df_results = pd.DataFrame(results, columns=["Airline", "Route", "Dep_Time", "Duration", "Price"])
                st.dataframe(df_results)
            else:
                st.write("No flights found for the selected route.")
elif user_option == 'Analytics':
    airline, frequency= db.fetch_airline_frequency()
    fig = go.Figure(
        plotly.graph_objects.Pie(
            labels=airline,
            values=frequency,
            hoverinfo="label+percent",
            textinfo="value"
        )

    )
    st.header("Pie chart")
    st.plotly_chart(fig)

    city, frequency1 = db.busy_airport()

    fig= plotly.express.bar(
        x=city,
        y=frequency1
    )
    
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

    date, frequency2 = db.daily_frequency()

    fig= plotly.express.line(
        x=date,
        y=frequency2
    )
    
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)

else:
    pass
