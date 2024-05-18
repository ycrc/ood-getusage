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

# unit into measurement value
meas_label = {"CPU Hours":"cpu_hours", "GPU Hours":"gpu_hours", "Service Units":"service_units"}

#---------------------------------------------------
# determine which group to show usage from (currently the Slurm default group, but
user = os.getenv('USER')
tmp = subprocess.run([f"/opt/slurm/current/bin/sacctmgr -P -n show association where users={user} format='account'"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
account = tmp.stdout.split('\n')


#---------------------------------------------------
server = flask.Flask(__name__)
# start Dash instance, needs the OOD prefix to properly set up React. This should be fixable to not be hard-coded...
app = Dash(server=server, requests_pathname_prefix="/pun/sys/ood-getusage/", external_stylesheets=[dbc.themes.BOOTSTRAP])

#---------------------------------------------------
# get data
# Mongodb connection
mongo_url = "mongodb://172.28.220.190:27017/"
conn = MongoClient(mongo_url)
db = conn.getusage
usage = db.usage
usage_log = db.usage_log



# build query
query = {"metadata.Account": {'$in':account}}

# submit query and convert into dataframe
df = pd.DataFrame(list(usage.find(query)))

# unpack metadata into columns
df = pd.concat([df[['timestamp','cpu_hours']],pd.DataFrame.from_records(df['metadata'])], axis=1)

df.rename(columns={'timestamp':'date'}, inplace=True)

for c in ['Account','Cluster','User','Partition']:
    df[c] = df[c].astype('string')

for c in ['cpu_hours']:
    df[c] = df[c].astype('float32')

#---------------------------------------------------
# Controls
controls = dbc.Card(
    [
        html.Div(
            [
                dbc.Label("Account"),
                dcc.Dropdown(
                    id="Account",
                    options=account,
                    value=account[0],
                ),
            ]
        ),
        html.Div(
            [
                dbc.Label("View"),
                dcc.Dropdown(
                    id="View",
                    options=["Partition", "User"],
                    value="Partition",
                ),
            ]
        ),
        html.Div(
            [
                dbc.Label("Units"),
                dcc.Dropdown(
                    id="Units",
                    #options=["CPU Hours", "GPU Hours", "Service Units"],
                    options=["CPU Hours"],
                    value="CPU Hours",
                ),
            ]
        ),
        html.Div(
            [
                dbc.Label("Partition Type"),
                dcc.Dropdown(
                    id="partition_class",
                    options=["All", "Commons", "Private", "Scavenge"],
                    value="All",
                ),
            ]
        ),
    ],
    body=True,
)

# Main layout
app.layout = dbc.Container([
    html.H1("YCRC Cluster Usage"),
    html.P("Utilization across all YCRC clusters."),
    html.Hr(),
    html.H2('Usage Summary'),
    html.Br(),
    dbc.Row([
        dbc.Col(html.Div(id='table'), md=5),
        dbc.Col(dcc.Graph(id='starburst'), md=7),
        ], justify='center',
    ),
    html.Hr(),
    html.Br(),
    html.H2("Monthly Breakdown"),
    dbc.Row([
        dbc.Col(controls, md=4),
        dbc.Col(dcc.Graph(id='monthly'), md=8),
        ], align='center'
    ),
    html.Hr(),
])


#---------------------------------------------------
@callback(
    Output('monthly', 'figure'),
    Output('starburst', 'figure'),
    Output('table', 'children'),
    Input('View', 'value'),
    Input('Account', 'value'),
    Input('Units', 'value'),
    Input('partition_class','value'),
)
def update_graph(view, account, units, partition_class):

    # bring in data frame
    global df

    if view is None or account is None or units is None:
        raise PreventUpdate
    else:

        tmp = df[df.Account == account]

        meas = meas_label[units]

        tmp['Commons'] = np.where(tmp['Partition'].str.contains('pi_|ycga|psych')==False, tmp[meas], 0)
        tmp['Scavenge'] = np.where(tmp['Partition'].str.contains('scavenge')==True, tmp[meas], 0)
        tmp['PI'] = np.where(tmp['Partition'].str.contains('pi_|ycga|psych')==True, tmp[meas], 0)

        if partition_class=="Commons":
            dff = tmp[tmp.Partition.str.contains('pi_')==False]
        elif partition_class=="Private":
            dff = tmp[tmp.Partition.str.contains('pi_')==True]
        elif partition_class=="Scavenge":
            dff = tmp[tmp.Partition.str.contains('scavenge')==True]
        else:
            dff = tmp

        fig1 = px.histogram(dff, x="date", y=meas, color=view, histfunc="sum")
        fig1.update_traces(xbins_size="M1")
        fig1.update_xaxes(showgrid=True, ticklabelmode="period", dtick="M1", tickformat="%b\n%Y")
        fig1.update_layout(bargap=0.1)

        tmp.date = pd.to_datetime(tmp.date)
        tmp = tmp.set_index('date')
        tmp = tmp.sort_index()
        dff = tmp.groupby([pd.Grouper(freq='ME'), 'User','Partition'], observed=True).cpu_hours.sum()
        dff = dff.reset_index()
        fig2 = px.sunburst(dff, path=['date', 'Partition', 'User'], values=meas, color='date')


        dff = tmp.groupby([pd.Grouper(freq='ME')], observed=True)[[meas,'Commons','PI','Scavenge']].sum()
        dff = dff.reset_index()
        dff = dff.sort_values(by='date')
        dff.date = dff.date.dt.strftime('%Y-%m')
        dff = dff.rename(columns={meas:'Total'})
        dff.Total = dff.Total.apply(lambda x: f'{x:.1f}')
        dff.Commons = dff.Commons.apply(lambda x: f'{x:.1f}')
        dff.PI = dff.PI.apply(lambda x: f'{x:.1f}')
        dff.Scavenge = dff.Scavenge.apply(lambda x: f'{x:.1f}')

        return fig1, fig2, dbc.Table.from_dataframe(dff, striped=True, bordered=True, hover=True)

