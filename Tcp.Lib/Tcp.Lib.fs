namespace Tcp

open System
open System.Text
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

  type IServer =
    inherit IDisposable
    abstract Connections: ConcurrentDictionary<Guid,IConnection>
    abstract Send: Guid -> byte array -> unit
    abstract Subscribe: (Guid * byte array -> unit) ->  IDisposable

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

    type Event =
      | Disconnect of Guid
      | Request    of Guid * byte array

    let private receiver (id: Guid) (client: TcpClient) (cb: Event -> unit) () =
      let stream = client.GetStream()
      while not (client.Client.Poll(1, SelectMode.SelectRead) && client.Client.Available = 0) do
        let buffer = Array.zeroCreate 512
        stream.Read(buffer,0,buffer.Length) |> ignore
        Event.Request(id, buffer) |> cb
        Thread.Sleep(1)
      id |> Event.Disconnect |> cb

    let create (client: TcpClient) (cb: Event -> unit) =
      let id = Guid.NewGuid()
      let cts = new CancellationTokenSource()
      let stream  = client.GetStream()

      let receiver: Thread = Thread(ThreadStart(receiver id client cb))
      receiver.Start()

      { new IConnection with
          member connection.Send (bytes: byte array) =
            stream.Write(bytes, 0, Array.length bytes)
            stream.Flush()

          member connection.Id
            with get () = id

          member connection.IPAddress
            with get () =
              let endpoint = client.Client.RemoteEndPoint :?> IPEndPoint
              endpoint.Address

          member connection.Dispose() =
            printfn "disposing %O" id
            cts.Cancel()
            cts.Dispose()
            receiver.Abort()
            stream.Close()
            stream.Dispose()
            client.Dispose() }

  //  ____
  // / ___|  ___ _ ____   _____ _ __
  // \___ \ / _ \ '__\ \ / / _ \ '__|
  //  ___) |  __/ |   \ V /  __/ |
  // |____/ \___|_|    \_/ \___|_|

  module Server =

    type Subscriptions = ConcurrentDictionary<Guid,IObserver<Guid * byte array>>
    type Connections = ConcurrentDictionary<Guid,IConnection>

    type private SharedState =
      { Connections: Connections
        Subscriptions: Subscriptions
        Socket: TcpListener }

    let private acceptor (state: SharedState) () =
      state.Socket.LocalEndpoint :?> IPEndPoint
      |> printfn "server now accepting connections on %O"

      let pending = ConcurrentQueue<Guid>()

      while true do
        while pending.Count > 0 do
          match pending.TryDequeue() with
          | true, id ->
            match state.Connections.TryRemove(id) with
            | true, connection ->
              connection.Dispose()
              printfn "removed: %O" id
            | false, _ -> ()
            printfn "open connections: %d" state.Connections.Count
          | false, _-> ()

        let client = state.Socket.AcceptTcpClient()

        let rec connection = Connection.create client <| function
          | Connection.Event.Disconnect id -> pending.Enqueue id
          | Connection.Event.Request (id,body) ->
            Encoding.UTF8.GetString(body)
            |> sprintf "id: %O sent: %s" id
            |> Encoding.UTF8.GetBytes
            |> fun body -> Observable.onNext state.Subscriptions (id, body)

        while not (state.Connections.TryAdd(connection.Id, connection)) do
          ignore ()

        printfn "added new connection from %O" connection.IPAddress

    let create (addr: IPAddress) (port: int) =
      let subscriptions = Subscriptions()
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
            try
              connections.[id].Send bytes
            with
              | exn ->
                exn.Message
                |> printfn "Error in Send: %s"

          member server.Subscribe (callback: Guid * byte array -> unit) =
            Observable.subscribe callback subscriptions

          member server.Dispose() =
            for KeyValue(_,connection) in connections.ToArray() do
              connection.Dispose()

            acceptor.Abort()
            listener.Stop() }
