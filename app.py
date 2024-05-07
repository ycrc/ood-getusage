from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate

import pandas as pd
from pymongo import MongoClient

markdown_text = '''
# YCRC CPU-hour Usage
Time-dependent usage metrics for YCRC clusters, in `cpu_hours`.
See our [docs page](http://docs.ycrc.yale.edu) for more information.
'''

app = Dash(__name__)

app.layout = html.Div([
    html.Div(children=[dcc.Markdown(markdown_text)]),
    html.Div(children=[
        html.Label('Account'),
        dcc.Input(id='Account', type='text', placeholder='Account', debounce=True),

        html.Br(),
        dcc.RadioItems(id='View', options=['Partition', 'User']),
        ], style={'padding': 10, 'flex': 1}),

    dcc.Graph(id='graph-content')
], style={'display': 'flex', 'flexDirection': 'column'})



@callback(
    Output('graph-content', 'figure'),
    Input('Account', 'value'),
    Input('View', 'value'),
)
def update_graph(account,view):

    if account is None or view is None:
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
    app.run()
