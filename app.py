

from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate
import flask

import pandas as pd
from pymongo import MongoClient

import os, subprocess



user = os.getenv('USER')
tmp = subprocess.run([f"/opt/slurm/current/bin/sacctmgr -P -n show user {user} format=DefaultAccount"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
account = tmp.stdout.strip()


server = flask.Flask(__name__)
app = Dash(server=server, requests_pathname_prefix="/pun/sys/ood-getusage/")

app.layout = html.Div([
    html.Div(html.H1(f'{account} Group CPU-Hour Usage:')),
    html.Div(html.P(f"Aggregated utilization (in cpu-hours) for all YCRC Clusters for the {account} group for {user}.")),
    html.Div(children=[
        html.Br(),
        dcc.RadioItems(id='View', options=['Partition', 'User'], value='Partition'),
        ], style={'padding': 10, 'flex': 1}),

    dcc.Graph(id='graph-content')
], style={'display': 'flex', 'flexDirection': 'column'})



@callback(
    Output('graph-content', 'figure'),
    Input('View', 'value'),
)
def update_graph(view):

    if view is None:
        raise PreventUpdate
    else:

        # Mongodb connection
        mongo_url = "mongodb://172.28.220.190:27017/"
        conn = MongoClient(mongo_url)
        db = conn.getusage
        usage = db.usage

        # build query
        query = {"metadata.Account":account}
        df = usage.find(query)

        # submit query and convert into dataframe
        df = pd.DataFrame(list(usage.find(query)))

        # unpack metadata into columns
        df = pd.concat([df[['timestamp','cpu_hours']],pd.DataFrame.from_records(df['metadata'])], axis=1)

        df.rename(columns={'timestamp':'date'}, inplace=True)
        df = df.set_index('date')
        df = df.sort_index()


        dff = df.groupby([pd.Grouper(freq='d'), view]).cpu_hours.sum()
        dff = dff.reset_index().set_index('date')
        return px.line(dff,x=dff.index, y='cpu_hours', color=view)

if __name__ == '__main__':
    app.run(host='172.28.220.190', port='8080', debug=True)

#if __name__ == "__main__":
#    run_simple("localhost", 8050, application)
