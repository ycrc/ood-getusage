from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import flask

import pandas as pd
from pymongo import MongoClient

import os, subprocess


# determine which group to show usage from (currently the Slurm default group, but
user = os.getenv('USER')
tmp = subprocess.run([f"/opt/slurm/current/bin/sacctmgr -P -n show association where users={user} format='account'"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
account = tmp.stdout.split('\n')


server = flask.Flask(__name__)
# start Dash instance, needs the OOD prefix to properly set up React. This should be fixable to not be hard-coded...
app = Dash(server=server, requests_pathname_prefix="/pun/sys/ood-getusage/", external_stylesheets=[dbc.themes.MORPH])

app.layout = html.Div([
    html.Div(html.H1(f'YCRC Cluster Usage:')),
    dcc.Dropdown(account, account[0], id="Account", placeholder="Account"),
    html.Div(html.P(f"Aggregated utilization (in cpu-hours) for all YCRC Clusters.")),
    html.Div(children=[
        html.Br(),
        dcc.RadioItems(id='View', options=['Partition', 'User'], value='Partition'),
        ], style={'padding': 10, 'flex': 1}),

    dcc.Graph(id='graph-content')
], style={'display': 'flex', 'flexDirection': 'column'})



@callback(
    Output('graph-content', 'figure'),
    Input('View', 'value'),
    Input('Account', 'value'),
)
def update_graph(view, account):

    if view is None or account is None:
        raise PreventUpdate
    else:

        # Mongodb connection
        mongo_url = "mongodb://172.28.220.190:27017/"
        conn = MongoClient(mongo_url)
        db = conn.getusage
        usage = db.usage

        # build query
        query = {"metadata.Account":account}

        # submit query and convert into dataframe
        df = pd.DataFrame(list(usage.find(query)))

        # unpack metadata into columns
        df = pd.concat([df[['timestamp','cpu_hours']],pd.DataFrame.from_records(df['metadata'])], axis=1)

        df.rename(columns={'timestamp':'date'}, inplace=True)

        #df = df.set_index('date')
        #df = df.sort_index()
        #dff = df.groupby([pd.Grouper(freq='ME'), view]).cpu_hours.sum()
        #dff = dff.reset_index().set_index('date')
        #fig = px.bar(dff,x=dff.index, y='cpu_hours', color=view, title='Monthly Usage (cpu-hours)')

        fig = px.histogram(df, x="date", y="cpu_hours", color=view, histfunc="sum", title=f"{account} monthly usage")
        fig.update_traces(xbins_size="M1")
        fig.update_xaxes(showgrid=True, ticklabelmode="period", dtick="M1", tickformat="%b\n%Y")
        fig.update_layout(bargap=0.1)

        return fig

