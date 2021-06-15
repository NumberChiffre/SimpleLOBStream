from collections import deque
import pickle
import dash
from plotly.graph_objs import Table, Scatter
from plotly.subplots import make_subplots
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import redis
import pandas as pd

from config import pairs

# set up dash
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# set up redis, default port/localhost
redis_conn = redis.StrictRedis()

# params for the dashboard
interval = 1
graph_len = int(0.5 * 60 * 60 // interval)
lob_depth = 10
graph_specs = [
    [{"type": "scatter"} for _ in pairs]
]
table_specs = [
    [{"type": "table"} for _ in pairs]
]


def add_dash():
    """Create dash layout for each pair with their order book table/historical
    spreads. Attach the callbacks for each table/graph separately with their own
    subscribe method to call the respective publisher by pair and attributes.

    Returns
    -------
        None
    """

    components = [
        html.Div(id='spreads_plotly'),
        dcc.Interval(
            id='tables-update',
            interval=interval * 1000,
            n_intervals=0
        )
    ]
    app.layout = html.Div(children=components)
    timestamps_queue = {pair: deque(maxlen=graph_len) for pair in pairs}
    spreads_queue = {pair: deque(maxlen=graph_len) for pair in pairs}

    def init_callback(app):
        """Dash callback functions matched based on ids to update Dash objects
        from data added to Redis.
        """

        @app.callback(
            Output('spreads_plotly', 'children'),
            [Input('tables-update', 'n_intervals')]
        )
        def update_quotes_balances_plotly(n):
            """Get the dict objects from Redis memory and update Plotly Table,
            where each table has a unique id based on its pair. The callback
            collects spreads/order book data from Redis memory, so any update
            will be updated per interval specified in config.
            """

            fig_quotes = make_subplots(
                rows=1,
                cols=len(pairs),
                subplot_titles=pairs,
                specs=table_specs
            )

            fig_hist_spreads = make_subplots(
                rows=1,
                cols=len(pairs),
                specs=graph_specs,
            )

            for idx, pair in enumerate(pairs):
                params = pickle.loads(redis_conn.hget(pair, f'{pair}_spread'))

                # convert dict objects into arrays for plotly tables.
                exchange_ts = params[f'{pair}_exchange_ts']
                spreads = params[f'{pair}_spread']
                lob = params[f'{pair}_orderbook']
                lob_d = dict(bids_price=list(), bids_quantity=list(),
                             asks_price=list(), asks_quantity=list())

                # limit depth number for displaying order book.
                for i, (bids, asks) in enumerate(
                        zip(lob['bids'].items(), lob['asks'].items())):
                    lob_d['bids_price'].append(round(float(bids[0]), 4))
                    lob_d['bids_quantity'].append(round(float(bids[1]), 4))
                    lob_d['asks_price'].append(round(float(asks[0]), 4))
                    lob_d['asks_quantity'].append(round(float(asks[1]), 4))
                    if i >= lob_depth - 1:
                        break

                lob_d = pd.DataFrame(lob_d)
                timestamps_queue[pair].append(exchange_ts)
                spreads_queue[pair].append(spreads)

                fig_quotes.add_trace(
                    Table(
                        header=dict(
                            values=[f'bids_price', f'bids_quantity',
                                    f'asks_price', f'asks_quantity'],
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align='center',
                            font=dict(color='white', size=8),
                        ),
                        cells=dict(
                            values=list(lob_d.T.values),
                            line_color='darkslategray',
                            fill_color='white',
                            align='center',
                        )
                    ),
                    row=1, col=idx + 1
                )

                fig_hist_spreads.add_trace(
                    Scatter(
                        x=list(timestamps_queue[pair]),
                        y=list(spreads_queue[pair]),
                        name='Historical Spreads',
                        mode='lines+markers',
                        marker=dict(color='royalblue')
                    ),
                    row=1, col=idx + 1
                )

                fig_quotes.update_layout(
                    width=1900,
                    height=300,
                    showlegend=False,
                    autosize=True,
                    font=dict(size=8),
                    margin=dict(b=0, t=20),
                )

                fig_hist_spreads.update_layout(
                    width=1900,
                    height=300,
                    showlegend=False,
                    autosize=True,
                    font=dict(size=8),
                    margin=dict(b=0, t=0),
                )
            dts = [
                html.Div([
                    dcc.Graph(id='quotes', figure=fig_quotes)
                ]),
                html.Div([
                    dcc.Graph(id='graphs', figure=fig_hist_spreads)
                ])
            ]
            return dts

    init_callback(app=app)
    return app


if __name__ == '__main__':
    app = add_dash()
    app.run_server(debug=False, dev_tools_ui=False, dev_tools_props_check=False)
