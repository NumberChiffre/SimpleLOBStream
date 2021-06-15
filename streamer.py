from typing import Callable, Dict
import json
from decimal import Decimal
from collections import namedtuple
import urllib.request
import urllib.parse
import asyncio
import websockets


# Define urls needed for order book streams
URL_ORDER_BOOK = "https://api.binance.com/api/v1/depth"
URL_ORDER_BOOK_PERP = "https://dapi.binance.com/dapi/v1/depth"

# OrderBook object
OrderBook = namedtuple("OrderBook", "bids asks")


def on_order_book(symbol: str, limit: int = 1000) -> OrderBook:
    """Get the full order book of a pair on Binance through public endpoint.

    Parameters
    ----------
    symbol: str
        Pair without underscore in between base/quote coin, e.g: BTCETH, ETHBRL.
    limit: int
        Maximum depth for the order book, which should be 1000 by default.

    Returns
    -------
    order_book: OrderBook
    """

    bids, asks = list(), list()
    attributes = {"symbol": symbol, "limit": limit}
    if '_' in symbol:
        url = f'{URL_ORDER_BOOK_PERP}?{urllib.parse.urlencode(attributes)}'
    else:
        url = f'{URL_ORDER_BOOK}?{urllib.parse.urlencode(attributes)}'

    try:
        req = urllib.request.Request(url, method='GET')
        resp = urllib.request.urlopen(req)
        res = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise Exception(f'{e.read()}')

    for bid in res['bids']:
        bids.append((Decimal(bid[0]), Decimal(bid[1])))
    for ask in res['asks']:
        asks.append((Decimal(ask[0]), Decimal(ask[1])))
    order_book = OrderBook(bids, asks)
    return order_book


class OrderBookStream:
    def __init__(self):
        """Handles order book updates every 1000ms by creating asynchronous
        tasks for each streaming object. Map objects to keep track of each
        streaming object with the symbol as key.
        """

        self._order_books = dict()
        self._order_books_perp = dict()
        self._sockets = set()
        self._tasks = dict()

    def get_order_book(self, symbol: str) -> Dict:
        return self._order_books[symbol]

    def get_order_book_perp(self, symbol: str) -> Dict:
        return self._order_books_perp[symbol]

    def update_order_book(self, symbol: str, updates: Dict):
        """With incoming stream updates for the order book, update this object
        by each depth. Overwrites existing depth level price with update's
        quantity or remove the depth level with no remaining quantity.

        Parameters
        ----------
        symbol: str
            Pair without underscore in between base/quote coin, e.g: BTCETH,
            ETHBRL.
        updates: Dict
            Dict containing order book updates.
        """

        # TODO: easiest way to differentiate spots from perpetuals
        if '_' in symbol:
            book = self._order_books_perp[symbol]
        else:
            book = self._order_books[symbol]

        # initialize the order book.
        if len(book['asks']) == 0 and len(book['bids']) == 0:
            order_book = on_order_book(symbol)
            for (p, q) in order_book.asks:
                book['asks'][p] = q
            for (p, q) in order_book.bids:
                book['bids'][p] = q

        # update order book with incoming updates across each depth.
        asks, bids = updates['a'], updates['b']
        for ask in asks:
            p, q = Decimal(ask[0]), Decimal(ask[1])
            if q > 0:
                book['asks'][p] = q
            elif p in book['asks']:
                del book['asks'][p]

        for bid in bids:
            p, q = Decimal(bid[0]), Decimal(bid[1])
            if q > 0:
                book['bids'][p] = q
            elif p in book['bids']:
                del book['bids'][p]

    def open_stream_order_book(self, symbol: str, callback: Callable):
        """Open order book stream for the given pair, provides callback function
        for the asynchronous task for streaming order book object which is used
        to process the updated depth levels of the order book.

        From Binance's API doc:
            The data in each event is the absolute quantity for a price level.
            If the quantity is 0, remove the price level. Receiving an event
            that removes a price level that is not in your local order book can
            happen and is normal.

        Parameters
        ----------
        symbol: str
            Pair without underscore in between base/quote coin, e.g: BTCETH,
            ETHBRL.
        callback: Callable
            Callback to handle the processing of the stream data.
        """

        self._order_books[symbol] = {'bids': {}, 'asks': {}}
        url = f'wss://stream.binance.com:9443/ws/{symbol.lower()}@depth'
        asyncio.Task(
            self.run(url=url, id=f'depth_{symbol.lower()}', callback=callback))

    def open_stream_order_book_perp(self, symbol: str, callback: Callable):
        """For perpetual only, open order book stream for the given pair,
        provides callback function for the asynchronous task for streaming order
        book object which is used to process the updated depth levels of the
        order book.

        From Binance's API doc:
            The data in each event is the absolute quantity for a price level.
            If the quantity is 0, remove the price level. Receiving an event
            that removes a price level that is not in your local order book can
            happen and is normal.

        Parameters
        ----------
        symbol: str
            Pair without underscore in between base/quote coin, e.g: BTCETH,
            ETHBRL.
        callback: Callable
            Callback to handle the processing of the stream data.
        """

        self._order_books_perp[symbol] = {'bids': {}, 'asks': {}}
        url = f'wss://dstream.binance.com/stream?streams={symbol.lower()}@depth'
        asyncio.Task(self.run(url=url, id=f'depth_perp_{symbol.lower()}',
                     callback=callback))

    async def run(self, url: str, id: str, callback: Callable):
        """Responsible for opening a stream for a given object, such as order
        book. Able to handle multiple streams and update the stored objects
        from incoming updates. Once the stored objects have been updated, the
        provided callback for the stream object is used for process/handle the
        updated object.

        Parameters
        ----------
        url: str
            URL.
        id: str
            Identifier for the object, which should refer to v1 url.
        callback: Callable
            Callback to handle the processing of the stream data.
        """

        # keeping track of streams to avoid duplicates.
        if id in self._sockets:
            print(f'Warning: socket {id} already opened!')
            return
        print(f'Starting stream: {url}')

        # keep track of opened sockets and async tasks.
        # process the updates based on stream object's ID.
        async with websockets.connect(url) as socket:
            self._sockets.add(id)
            while id in self._sockets:
                recv_task = asyncio.Task(socket.recv())
                self._tasks[id] = recv_task
                data = await recv_task
                data = json.loads(data)
                del self._tasks[id]
                if id.find('depth') == 0:
                    if id.find('depth_perp') == 0:
                        data = data['data']
                    symbol = data['s']
                    self.update_order_book(symbol=symbol, updates=data)
                callback(data)
                await(asyncio.sleep(.1))

    def close(self):
        """Close all streams upon keyboard interruption for all async tasks used
        for streaming, also doable by setting a timer for the duration of the
        streams.
        """

        print('closing all streams')
        for key in self._tasks:
            self._tasks[key].cancel()
        self._sockets.clear()
        print('closed all streams')
