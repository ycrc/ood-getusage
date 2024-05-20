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
# determine which accounts to show usage from
user = os.getenv('USER')
cmd = f"/opt/slurm/current/bin/sacctmgr -P -n show association where users={user} format='account'"
tmp = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
account = tmp.stdout.split('\n')

#---------------------------------------------------
server = flask.Flask(__name__)
# start Dash instance, needs the OOD prefix to properly set up React. This should be fixable to not be hard-coded...
app = Dash(server=server, requests_pathname_prefix="/pun/sys/ycrc_getusage/", external_stylesheets=[dbc.themes.BOOTSTRAP])

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

df = df.set_index('date')
df = df.sort_index()
df = df.groupby([pd.Grouper(freq='ME'),'Account','Cluster','User','Partition']).cpu_hours.sum()
df = df.reset_index()
df = df.set_index('date')

#---------------------------------------------------
# Controls
controls = dbc.Card(
    [
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
                dbc.Label("Partition Type"),
                dcc.Dropdown(
                    id="partition_class",
                    options=["All", "Commons", "Private", "Scavenge"],
                    value="All",
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

    ],
    body=True,
)

# Main layout
app.layout = dbc.Container([
    html.H1("YCRC Cluster Usage"),
    dbc.Row(
        [
        dbc.Col(dbc.Label(html.H4("Account:")), width='auto'),
        dbc.Col(dcc.Dropdown(
            id="Account",
            options=account,
            value=account[0],
        ), width=5),
        dbc.Col(
            [
                html.Button("Download Detailed Usage Report", id='btn_csv'),
                dcc.Download(id='download-df'),
            ],
        ),
        ]
    ),
    html.Hr(),
    html.H2('Usage Summary'),
    html.P("Latest month is in-progress (data updated daily at midnight)."),
    html.Br(),
    dbc.Row([
        dbc.Col(dbc.Label(html.H4("Usage per Month (cpu-hours)")),md=6),
        dbc.Col(dbc.Label(html.H4("FY24 Usage per User (cpu-hours)")), md=5),
        ], justify='evenly',
    ),
    dbc.Row([
        dbc.Col(html.Div(id='table_monthly'), md=6),
        dbc.Col(html.Div(id='table_user'), md=5, style={"maxHeight":"570px", "overflow":"scroll"}),
        ], justify='evenly',
    ),
    html.Hr(),
    html.Br(),
    html.H2("Monthly Breakdown"),
    dbc.Row([
        dbc.Col(controls, md=3),
        dbc.Col(dcc.Graph(id='monthly'), md=9),
        ], align='center'
    ),
    html.Hr(),
])

#---------------------------------------------------
@callback(
    Output('download-df', 'data'),
    Input('btn_csv',"n_clicks"),
    prevent_initial_call=True,
)
def download_df(n_clicks):
    global df
    cols = []
    if df.Account.nunique() > 1:
        cols.append('Account')
    elif df.Cluster.nunique() > 1:
        cols.append('Cluster')
    cols += ["Partition","User","cpu_hours"]
    tmp = df[cols]
    tmp = tmp.reset_index()
    tmp = tmp.sort_values(by='date')
    tmp.date = tmp.date.dt.strftime('%Y-%m')
    tmp.cpu_hours = tmp.cpu_hours.apply(lambda x: f'{x:.1f}')
    return dcc.send_data_frame(tmp.to_csv, "usage_report.csv", encoding='utf-8', index=False)



#---------------------------------------------------
@callback(
    Output('monthly', 'figure'),
    Output('table_monthly', 'children'),
    Output('table_user', 'children'),
    Input('View', 'value'),
    Input('Account', 'value'),
    Input('Units', 'value'),
    Input('partition_class','value'),
)
def update(view, account, units, partition_class):

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
            dff = tmp[tmp.Partition.str.contains('pi_|ycga|psych')==False]
        elif partition_class=="Private":
            dff = tmp[tmp.Partition.str.contains('pi_|ycga|psych')==True]
        elif partition_class=="Scavenge":
            dff = tmp[tmp.Partition.str.contains('scavenge')==True]
        else:
            dff = tmp

        fig1 = px.histogram(dff, x=dff.index, y=meas, color=view, histfunc="sum")
        fig1.update_traces(xbins_size="M1")
        fig1.update_xaxes(showgrid=True, ticklabelmode="period", dtick="M1", tickformat="%b\n%Y")
        fig1.update_layout(bargap=0.1)

        # tmp.date = pd.to_datetime(tmp.date)
        # tmp = tmp.set_index('date')
        # tmp = tmp.sort_index()

        dff = tmp.groupby([pd.Grouper(freq='ME')])[[meas,'Commons','PI','Scavenge']].sum()
        dff = dff.reset_index()
        dff = dff.sort_values(by='date')
        dff.date = dff.date.dt.strftime('%Y-%m')
        summary = dff.sum()
        summary['date'] = "Total"
        dff.loc[len(dff)] = summary
        dff = dff.rename(columns={meas:'Total'})
        dff.Total = dff.Total.apply(lambda x: f'{x:,.1f}')
        dff.Commons = dff.Commons.apply(lambda x: f'{x:,.1f}')
        dff.PI = dff.PI.apply(lambda x: f'{x:,.1f}')
        dff.Scavenge = dff.Scavenge.apply(lambda x: f'{x:,.1f}')
        t_m = dbc.Table.from_dataframe(dff, striped=True, bordered=True, hover=True)

        dff = tmp.groupby(['User'])[[meas,'Commons','PI','Scavenge']].sum()
        dff = dff.reset_index()
        dff = dff.sort_values(by='User')
        dff = dff.rename(columns={meas:'Total'})
        dff.Total = dff.Total.apply(lambda x: f'{x:,.1f}')
        dff.Commons = dff.Commons.apply(lambda x: f'{x:,.1f}')
        dff.PI = dff.PI.apply(lambda x: f'{x:,.1f}')
        dff.Scavenge = dff.Scavenge.apply(lambda x: f'{x:,.1f}')
        t_u = dbc.Table.from_dataframe(dff, striped=True, bordered=True, hover=True)

        return fig1, t_m, t_u

