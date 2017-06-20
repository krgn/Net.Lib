namespace Tcp
open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.Collections.Concurrent

[<AutoOpen>]
module Lib =

  let private tag (mdl: string) (tag: string) =
    String.Format("Tcp.{0}.{1}", mdl, tag)

  //  _____
  // |_   _|   _ _ __   ___  ___
  //   | || | | | '_ \ / _ \/ __|
  //   | || |_| | |_) |  __/\__ \
  //   |_| \__, | .__/ \___||___/
  //       |___/|_|

  type IClientSocket =
    inherit IDisposable
    abstract Id: Guid
    abstract Send: byte array -> unit
    abstract Subscribe: (byte array -> unit) ->  IDisposable

  type IConnection =
    inherit IDisposable
    abstract Send: byte array -> unit
    abstract Id: Guid
    abstract IPAddress: IPAddress
    abstract Subscribe: (byte array -> unit) -> IDisposable

  type IServer =
    inherit IDisposable
    abstract Connections: ConcurrentDictionary<Guid,IConnection>
    abstract Send: Guid -> byte array -> unit
    abstract Subscribe: (byte array -> unit) ->  IDisposable

  //   ____ _ _            _
  //  / ___| (_) ___ _ __ | |_
  // | |   | | |/ _ \ '_ \| __|
  // | |___| | |  __/ | | | |_
  //  \____|_|_|\___|_| |_|\__|

  module Client =

    let create (addr: string) (port: int) =
      let id = Guid.NewGuid()

      let receiver: Thread = Unchecked.defaultof<Thread>

      { new IClientSocket with
          member socket.Send(bytes) =
            failwith "Send"

          member socket.Id
            with get () = id

          member socket.Subscribe (callback: byte array -> unit) =
            failwith "Subscribe"

          member socket.Dispose () =
            failwith "Dispose" }

  //   ____                            _   _
  //  / ___|___  _ __  _ __   ___  ___| |_(_) ___  _ __
  // | |   / _ \| '_ \| '_ \ / _ \/ __| __| |/ _ \| '_ \
  // | |__| (_) | | | | | | |  __/ (__| |_| | (_) | | | |
  //  \____\___/|_| |_|_| |_|\___|\___|\__|_|\___/|_| |_|

  module private Connection =

    let create (client: TcpClient) =
      let id = Guid.NewGuid()

      let stream  = client.GetStream()
      let receiver: Thread = Unchecked.defaultof<Thread>

      { new IConnection with
          member connection.Send (bytes: byte array) =
            stream.Write(bytes, 0, Array.length bytes)
            stream.Flush()

          member connection.Id
            with get () = id

          member connection.Subscribe callback =
            failwith "Subscribe"

          member connection.IPAddress
            with get () =
              let endpoint = client.Client.RemoteEndPoint :?> IPEndPoint
              endpoint.Address

          member connection.Dispose() =
            client.Dispose() }

  //  ____
  // / ___|  ___ _ ____   _____ _ __
  // \___ \ / _ \ '__\ \ / / _ \ '__|
  //  ___) |  __/ |   \ V /  __/ |
  // |____/ \___|_|    \_/ \___|_|

  module Server =

    type Subscriptions = ConcurrentDictionary<Guid,IObservable<byte array>>
    type Connections = ConcurrentDictionary<Guid,IConnection>

    type private SharedState =
      { Connections: Connections
        Subscriptions: Subscriptions
        Socket: TcpListener }

    let private acceptor (state: SharedState) () =
      while true do
        let client = state.Socket.AcceptTcpClient()
        let connection = Connection.create client

        while not (state.Connections.TryAdd(connection.Id, connection)) do
          ignore ()

        printfn "added new connection from %O" connection.IPAddress

    let create (addr: IPAddress) (port: int) =
      let subscriptions = ConcurrentDictionary<Guid,IObservable<byte array>>()
      let connections = ConcurrentDictionary<Guid,IConnection>()

      let listener = TcpListener(addr, port)
      listener.Start()

      let state =
        { Socket = listener
          Connections = connections
          Subscriptions = subscriptions }

      let acceptor: Thread = Thread(ThreadStart(acceptor state))

      acceptor.Start()

      { new IServer with
          member server.Connections
            with get () = connections

          member server.Send (id: Guid) (bytes: byte array) =
            failwith "Send"

          member server.Subscribe (callback: byte array -> unit) =
            failwith "Subscribe"

          member server.Dispose() =
            acceptor.Abort()
            listener.Stop() }
