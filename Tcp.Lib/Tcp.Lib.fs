namespace Tcp

open System
open System.Text
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
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

    type Event =
      | Disconnected
      | Response of byte array

    type private Subscriptions = ConcurrentDictionary<Guid,IObserver<Event>>

    type private SharedState =
      { Socket: TcpClient
        Subscriptions: Subscriptions }

    let private receiver (state: SharedState) () =
      let stream = state.Socket.GetStream()

      while not (state.Socket.Client.Poll(1, SelectMode.SelectRead) && state.Socket.Client.Available = 0) do
        printfn "yeeeeeah"

      Observable.onNext state.Subscriptions Disconnected

    let create (addr: IPAddress) (port: int) =
      let id = Guid.NewGuid()

      let subscriptions = Subscriptions()

      let state = Unchecked.defaultof<SharedState>

      let receiver: Thread = Thread(ThreadStart(receiver state))

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

  //     _                         ____
  //    / \   ___ _   _ _ __   ___/ ___|  ___ _ ____   _____ _ __
  //   / _ \ / __| | | | '_ \ / __\___ \ / _ \ '__\ \ / / _ \ '__|
  //  / ___ \\__ \ |_| | | | | (__ ___) |  __/ |   \ V /  __/ |
  // /_/   \_\___/\__, |_| |_|\___|____/ \___|_|    \_/ \___|_|
  //              |___/

  module AsyncServer =

    type private Subscriptions = ConcurrentDictionary<Guid,IObserver<Guid * byte array>>

    type Connections = ConcurrentDictionary<Guid,IConnection>

    type private SharedState =
      { Connections: Connections
        Subscriptions: Subscriptions
        Socket: TcpListener }

    type private SocketState() as state =
      let bufSize = 1024
      let buf: byte[] = Array.zeroCreate bufSize
      let builder = StringBuilder()

      let rec loop () =  async {
          do! Async.Sleep(1000)
          try
            if not (state.Socket.Poll(1, SelectMode.SelectRead) && state.Socket.Available = 0) then
              printfn "still connected"
              return! loop()
          with
            | exn ->
              printfn "disconnected"
              try
                state.Socket.Shutdown(SocketShutdown.Both)
                state.Socket.Close()
              with
                | _ -> ()
              state.Socket.Dispose()
        }

      do Async.Start(loop())

      // Client  socket.
      [<DefaultValue>] val mutable Socket:Socket

      member state.BufferSize with get () = bufSize

      // Receive buffer.
      member state.Buffer
        with get () = buf

      member state.Builder
        with get () = builder

    let sendCallback (result: IAsyncResult) =
      try
        // Retrieve the socket from the state object.
        let handler = result.AsyncState :?> Socket

        // Complete sending the data to the remote device.
        let bytesSent = handler.EndSend(result)

        printfn "Sent %d bytes to client." bytesSent

        handler.Shutdown(SocketShutdown.Both)
        handler.Close()

      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let send (socket: Socket) (data: string) =
      // Convert the string data to byte data using ASCII encoding.
      let byteData = Encoding.UTF8.GetBytes(data)

      // Begin sending the data to the remote device.
      socket.BeginSend(
        byteData,
        0,
        byteData.Length,
        SocketFlags.None,
        AsyncCallback(sendCallback),
        socket)
      |> ignore

    let rec receiveCallback (result: IAsyncResult) =
      let mutable content = String.Empty

      // Retrieve the state object and the handler socket
      // from the asynchronous state object.
      let state = result.AsyncState :?> SocketState
      let handler = state.Socket

      // Read data from the client socket.
      let bytesRead = handler.EndReceive(result)

      if bytesRead > 0 then
        // There  might be more data, so store the data received so far.
        state.Builder.Append(Encoding.UTF8.GetString(state.Buffer,0,bytesRead)) |> ignore

        // Check for end-of-file tag. If it is not there, read
        // more data.
        content <- state.Builder.ToString()
        if (content.IndexOf("<EOF>") > -1) then
          // All the data has been read from the
          // client. Display it on the console.
          printfn "Read %d bytes from socket. \n Data : %s" content.Length content
          // Echo the data back to the client.
          send handler content
        else
          // Not all data received. Get more.
          handler.BeginReceive(
            state.Buffer,
            0,
            state.BufferSize,
            SocketFlags.None,
            AsyncCallback(receiveCallback),
            state)
          |> ignore


    let start (addr: IPAddress) (port: int) =
      let subscriptions = Subscriptions()
      let connections = ConcurrentDictionary<Guid,IConnection>()
      let allDone = new ManualResetEvent(false)
      let listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
      let endpoint = IPEndPoint(addr, port)

      listener.Bind(endpoint)
      listener.Listen(100)

      let acceptCallback (result: IAsyncResult) =
        allDone.Set() |> ignore

        let listener = result.AsyncState :?> Socket
        let handler = listener.EndAccept(result)
        let state = SocketState()
        state.Socket <- handler

        handler.BeginReceive(
          state.Buffer,
          0,
          state.BufferSize,
          SocketFlags.None,
          AsyncCallback(receiveCallback),
          state)
        |> ignore

      let acceptor () =
        while true do
          allDone.Reset() |> ignore
          printfn "Waiting for new connections"
          listener.BeginAccept(AsyncCallback(acceptCallback), listener) |> ignore
          allDone.WaitOne() |> ignore

      let thread = Thread(ThreadStart(acceptor))
      thread.Start()

      { new IDisposable with
          member self.Dispose() =
            thread.Abort()
            try
              listener.Shutdown(SocketShutdown.Both)
              listener.Close()
            with
              | _ -> ()
            listener.Dispose() }
