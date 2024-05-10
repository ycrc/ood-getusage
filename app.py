from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import flask

import pandas as pd
from pymongo import MongoClient
import numpy as np


import os, subprocess


# determine which group to show usage from (currently the Slurm default group, but
user = os.getenv('USER')
tmp = subprocess.run([f"/opt/slurm/current/bin/sacctmgr -P -n show association where users={user} format='account'"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
account = tmp.stdout.split('\n')


server = flask.Flask(__name__)
# start Dash instance, needs the OOD prefix to properly set up React. This should be fixable to not be hard-coded...
app = Dash(server=server, requests_pathname_prefix="/pun/sys/ood-getusage/", external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dbc.Row(dbc.Col(html.H1("YCRC Cluster Usage"), width={"size":6, "offset":1})),
    dbc.Row(dbc.Col(dcc.Dropdown(account, account[0], id='Account'), width={"size":6, "offset":1})),
    dbc.Row([
        dbc.Col(dcc.Dropdown(["Partition","User"], "Partition", id='View',), width=2, align='center'),
        dbc.Col(dcc.Graph(id='monthly'), width=8),
        ], justify='center'
    ),
    dbc.Row(
        [dbc.Col(html.Div(id='table'), width='auto'),
        dbc.Col(dcc.Graph(id='starburst'), width=6)],
        justify='center',
    ),

])


@callback(
    Output('monthly', 'figure'),
    Output('starburst', 'figure'),
    Output('table', 'children'),
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
        df['Commons'] = np.where(df['Partition'].str.contains('pi_|ycga|psych')==False, df['cpu_hours'], 0)
        df['Scavenge'] = np.where(df['Partition'].str.contains('scavenge')==True, df['cpu_hours'], 0)
        df['PI'] = np.where(df['Partition'].str.contains('pi_|ycga|psych')==True, df['cpu_hours'], 0)

        fig1 = px.histogram(df, x="date", y="cpu_hours", color=view, histfunc="sum", title=f"{account} monthly usage")
        fig1.update_traces(xbins_size="M1")
        fig1.update_xaxes(showgrid=True, ticklabelmode="period", dtick="M1", tickformat="%b\n%Y")
        fig1.update_layout(bargap=0.1)

        df.date = pd.to_datetime(df.date)
        df = df.set_index('date')
        df = df.sort_index()
        dff = df.groupby([pd.Grouper(freq='ME'), 'User','Partition']).cpu_hours.sum()
        dff = dff.reset_index()
        fig2 = px.sunburst(dff, path=['date', 'Partition', 'User'], values='cpu_hours', color='date')


        dff = df.groupby([pd.Grouper(freq='ME')])[['cpu_hours','Commons','PI','Scavenge']].sum()
        dff = dff.reset_index()
        dff = dff.sort_values(by='date')
        dff.date = dff.date.dt.strftime('%Y-%m')
        dff = dff.rename(columns={'cpu_hours':'Total'})
        dff.Total = dff.Total.apply(lambda x: f'{x:.1f}')
        dff.Commons = dff.Commons.apply(lambda x: f'{x:.1f}')
        dff.PI = dff.PI.apply(lambda x: f'{x:.1f}')
        dff.Scavenge = dff.Scavenge.apply(lambda x: f'{x:.1f}')

        return fig1, fig2, dbc.Table.from_dataframe(dff, striped=True, bordered=True, hover=True)

