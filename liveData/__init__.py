#!/bin/python3 


"""
This module provides methods for retrieving auction data from Alpaca.Markets and posting it
to a socket server as a serialized JSON object.

Run the daemon by typing:

```
python3 -m Lab93BackendAPI --live-data \
                           --trade-pairs BTC/USD \
                           --host 127.0.0.1 \
                           --port 65535
```

"""


# Access tools for connecting to the api Hub.
from socket import socket, AF_INET, SOCK_STREAM

# Argument handling for shell scripting.
from argparse import ArgumentParser

# JSON serialization for handling data objects.
from json import dumps as serialize

from time import sleep
from datetime import datetime
from threading import Thread

# In-House tools for dealing with databases and encryption.
from Lab93Cryptogram import CryptographyMethodsAPI as cryptogram
from Lab93DatabaseSystem.submodules.DatabaseAPI import SQLite3 as SQL

# Alpaca.Markets SDK
from alpaca.data.live import CryptoDataStream as CryptoStream
from alpaca.data.live import StockDataStream  as StocksStream

# Logging tools.
from logging import basicConfig, getLogger
from logging import INFO, DEBUG
from logging import info as information
from logging import debug as debugging


class liveDataServer:
    """
    The live data server uses a socket connection to retrieve live auction
    data from the Alpaca.Markets API and redirect the results to another
    socket server for interpretation by any other systems within the host.
    """


    # Set up our logger.
    getLogger()


    def __init__( self,
                  keyfile:   str="/server/credentials/admin.key",
                  database:  str="/server/database/admin.db",
                  symbols:   list=["BTC/USD"],
                  host:      str="127.0.0.1",
                  port:      int=65535                ):

        information("Initializing auction data collection daemon.")


        # Set runtime constants.
        debugging("Establishing server constants.")

        self.keyfile     = keyfile  # Filepath to SSH key.
        self.database    = database # DB containing API keys.
        self.host        = host     # The IP of the socket server.
        self.port        = port     # Port for the socket server.
        self.data        = {}       # Dictionary containing our results.

        # Gather the secret API credentials from the database.
        # Use the SSH key as the encryption key for the secrets.
        self.credentials = SQL.databaseInterface().queryCredentials("/server/database/admin/admin.db")

        # Initialize the Alpaca.Markets live data client.
        self.client      = CryptoStream( self.credentials[0],
                                         self.credentials[1]  )

        # Subscribe to each of the asset pairs provided by the symbols list.
        debugging("Subscribing to asset pairs.")
        for asset in symbols: self.client\
                                  .subscribe_quotes( self.streamDataHandler,
                                                     str(  asset.upper() )   )

        # Begin running the data client in the background.
        debugging("Begin runtime execution.")
        Thread( daemon=True,
                name="CryptoClient",
                target=self.client\
                           .run()    ).start()


    async def streamDataHandler( self, data ):
        """
        Recieves new data from the market stream and redirects it to
        a local socket server for use by other facilities in the lab.
        """

        information( f"Recieved live auction data for: {data.symbol}" )
        debugging( f"Data Recieved:\n{data}\n\n" )


        # Re-Write the sub-dictionary inside the self.data dictionary.
        debugging("Updating data packet contents.")
        self.data["Live Market Data"] =  {
            str(data.symbol): {
                "name":      data.symbol,
                "time":      datetime.timestamp( data.timestamp ),
                "ask price": data.ask_price,
                "ask size":  data.ask_size,
                "bid price": data.bid_price,
                "bid size":  data.bid_size
            }
        }


        # Connect to the socket server and send the data packet.
        debugging( "Connecting to the api hub." )
        with socket( AF_INET, SOCK_STREAM ) as server:


            # Establish connection.
            debugging( "Attempting connection." )
            try:
                server.connect( (self.host, self.port) )
                debugging( "Connection established." )

            except Exception as error:
                exception(
                    f"There was an issue connecting to the api hub;\n{error}"
                ); return error


            # Upload data packet.
            debugging("Attempting upload to hub.")
            try:
                server.sendall( bytes( serialize( self.data ),
                                       encoding="utf-8"        ) )

                debugging( "Upload complete." )

            except Exception as error:
                exception(
                    f"There was an issue uploading data to the api hub;"
                    f"\n{error}"
                ); return error

            # Close the connection
            finally:
                server.close()
                sleep(1)


if __name__ == "__main__":

    arguments = ArgumentParser()


    arguments.add_argument( "-T", "--trade-pairs", 
                            nargs="+", default=["BTC/USD"],
                            help="Add trading pairs of cryptocurrency to track." )

    arguments.add_argument( "-L",
                            "--logfile",
                            default="/server/logs/liveData.log",
                            help="Specify a filepath to redirect log output to." )

    arguments.add_argument( "-P", 
                            "--port",
                            default=65535,
                            help="Designate a port for the hub server." )

    arguments.add_argument( "-H",
                            "--host",
                            default="127.0.0.1",
                            help="Designate an IP address for the hub server.")


    arguments = arguments.parse_args()


    basicConfig(filename=arguments.logfile, level=INFO)

    liveDataServer(symbols=arguments.trade_pairs, port=int(arguments.port))
