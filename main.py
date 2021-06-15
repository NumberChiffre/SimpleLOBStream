from typing import Dict, List
from datetime import datetime
import pickle
import asyncio

import streamer
from monitor import redis_conn
from config import pairs


def run(pairs: List[str]) -> None:
    """Use redis stream to send over ts/spread/order book data.

    Parameters
    ----------
    pairs: List[str]
        Array containing the pairs that we want to stream.
    """

    stream = streamer.OrderBookStream()

    def call_order_book(data: Dict):
        """Callback function that stores the spread and the order book of the
        given pair into redis via hash set, which can be retrieved in the
        dash callback function for monitoring.

        Parameters
        ----------
        data: Changes to the orderbook/trade objects.
            Incoming stream changes to the orderbook/trade objects.
        """

        ts = datetime.fromtimestamp(data['E'] / 1000)

        if '_' in data['s']:
            lob = stream.get_order_book_perp(data['s'])
        else:
            lob = stream.get_order_book(data['s'])

        # TODO: redundant sanity check though, remove.
        lob['asks'] = dict(sorted(lob['asks'].items()))
        lob['bids'] = dict(sorted(lob['bids'].items(), reverse=True))

        # generate spread
        spread = round(float(list(lob['asks'].keys())[0]) - float(
            list(lob['bids'].keys())[0]), 4)

        # pass on the spread/order book data to the dashboard
        dash_params = dict()
        dash_params[f"{data['s']}_exchange_ts"] = ts
        dash_params[f"{data['s']}_spread"] = spread
        dash_params[f"{data['s']}_orderbook"] = lob
        redis_conn.hset(data['s'], f"{data['s']}_spread",
                        pickle.dumps(dash_params))

    # For each pair, start streaming the order book object, close all streams
    # upon keyboard interruption to stop the program.
    for pair in pairs:
        if '_' in pair:
            stream.open_stream_order_book_perp(pair, call_order_book)
        else:
            stream.open_stream_order_book(pair, call_order_book)
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        stream.close()
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    run(pairs)
