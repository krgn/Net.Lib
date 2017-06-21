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

  type ClientEvent =
    | Connected
    | Disconnected
    | Response of byte array

  type IClientSocket =
    inherit IDisposable
    abstract Id: Guid
    abstract Send: byte array -> unit
    abstract Subscribe: (ClientEvent -> unit) ->  IDisposable

  type ServerEvent =
    | Connect    of id:Guid * ip:IPAddress * Port:int
    | Disconnect of id:Guid
    | Request    of id:Guid * request:byte array

  type Subscriptions = ConcurrentDictionary<Guid,IObserver<ServerEvent>>

  type IConnection =
    inherit IDisposable
    abstract Socket: Socket
    abstract Send: byte array -> unit
    abstract Id: Guid
    abstract IPAddress: IPAddress
    abstract Port: int
    abstract Buffer: byte array
    abstract BufferSize: int
    abstract Subscriptions: Subscriptions

  type Connections = ConcurrentDictionary<Guid,IConnection>

  type IServer =
    inherit IDisposable
    abstract Connections: Connections
    abstract Send: Guid -> byte array -> unit
    abstract Subscribe: (ServerEvent -> unit) ->  IDisposable

  //  _   _ _   _ _
  // | | | | |_(_) |___
  // | | | | __| | / __|
  // | |_| | |_| | \__ \
  //  \___/ \__|_|_|___/

  module Socket =

    let isAlive (socket:Socket) =
      not (socket.Poll(1, SelectMode.SelectRead) && socket.Available = 0)

    let dispose (socket:Socket) =
      try
        socket.Shutdown(SocketShutdown.Both)
        socket.Close()
      finally
        socket.Dispose()

  //   ____ _ _            _
  //  / ___| (_) ___ _ __ | |_
  // | |   | | |/ _ \ '_ \| __|
  // | |___| | |  __/ | | | |_
  //  \____|_|_|\___|_| |_|\__|

  module rec Client =

    type private Subscriptions = ConcurrentDictionary<Guid,IObserver<ClientEvent>>

    type private IState =
      inherit IDisposable
      abstract Socket: Socket
      abstract EndPoint: IPEndPoint
      abstract Connected: ManualResetEvent
      abstract Sent: ManualResetEvent
      abstract Buffer: byte array
      abstract BufferSize: int
      abstract Response: ResizeArray<byte>
      abstract Subscriptions: Subscriptions

    let private connectCallback (ar: IAsyncResult) =
      try
        let state = ar.AsyncState :?> IState

        // Complete the connection.
        state.Socket.EndConnect(ar)

        printfn "Socket connected to %O" state.EndPoint

        // Signal that the connection has been made.
        state.Connected.Set() |> ignore
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    // Connect to the remote endpoint.
    let private beginConnect (state: IState) =
      state.Socket.BeginConnect(state.EndPoint, AsyncCallback(connectCallback), state) |> ignore
      state.Connected.WaitOne() |> ignore

    let private receiveCallback (ar: IAsyncResult) =
      try
        // Retrieve the state object and the client socket
        // from the asynchronous state object.
        let state = ar.AsyncState :?> IState

        // Read data from the remote device.
        let bytesRead = state.Socket.EndReceive(ar)

        if bytesRead > 0 then
          if bytesRead = state.BufferSize then
            state.Response.AddRange state.Buffer
          else
            let intermediate = Array.zeroCreate bytesRead
            Array.blit state.Buffer 0 intermediate 0 bytesRead
            state.Response.AddRange intermediate
          receive state
        else
          // All the data has arrived; put it in response.
          if state.Response.Count > 0 then
            state.Response.ToArray()
            |> ClientEvent.Response
            |> Observable.onNext state.Subscriptions
            state.Response.Clear()
          receive state
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let private receive (state: IState) =
      try
        // Begin receiving the data from the remote device.
        state.Socket.BeginReceive(
          state.Buffer,
          0,
          state.BufferSize,
          SocketFlags.None,
          AsyncCallback(receiveCallback),
          state)
        |> ignore
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let private sendCallback (ar: IAsyncResult) =
      try
        let state = ar.AsyncState :?> IState

        // Complete sending the data to the remote device.
        let bytesSent = state.Socket.EndSend(ar)

        printfn "Sent %d bytes to server." bytesSent

        // Signal that all bytes have been sent.
        state.Sent.Set() |> ignore
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let private send (state: IState) (data: byte array) =
      try
        // Begin sending the data to the remote device.
        state.Socket.BeginSend(
          data,
          0,
          data.Length,
          SocketFlags.None,
          AsyncCallback(sendCallback),
          state)
        |> ignore
        state.Sent.WaitOne() |> ignore
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let create (addr: IPAddress) (port: int) =
      let id = Guid.NewGuid()

      let endpoint = IPEndPoint(addr, port)
      let client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)

      let state =
        let bufSize = 256
        let buffer = Array.zeroCreate bufSize
        let connected = new ManualResetEvent(false)
        let sent = new ManualResetEvent(false)
        let response = ResizeArray()
        let subscriptions = Subscriptions()
        { new IState with
            member state.Socket
              with get () = client

            member state.EndPoint
              with get () = endpoint

            member state.Connected
              with get () = connected

            member state.Sent
              with get () = sent

            member state.Buffer
              with get () = buffer

            member state.BufferSize
              with get () = bufSize

            member state.Response
              with get () = response

            member state.Subscriptions
              with get () = subscriptions

            member state.Dispose() =
              Socket.dispose client
          }

      beginConnect state

      { new IClientSocket with
          member socket.Send(bytes) =
            // Send test data to the remote device.
            send state bytes

          member socket.Id
            with get () = id

          member socket.Subscribe (callback: ClientEvent -> unit) =
            Observable.subscribe callback state.Subscriptions

          member socket.Dispose () =
            state.Dispose() }


  //  ____  _                        _
  // / ___|| |__   __ _ _ __ ___  __| |
  // \___ \| '_ \ / _` | '__/ _ \/ _` |
  //  ___) | | | | (_| | | |  __/ (_| |
  // |____/|_| |_|\__,_|_|  \___|\__,_|

  module private Shared =

    type IState =
      inherit IDisposable
      abstract DoneSignal: ManualResetEvent
      abstract Connections: Connections
      abstract Subscriptions: Subscriptions
      abstract Listener: Socket

    let create (socket: Socket) =
      let signal = new ManualResetEvent(false)
      let connections = Connections()
      let subscriptions = Subscriptions()

      { new IState with
          member state.DoneSignal
            with get () = signal

          member state.Connections
            with get () = connections

          member state.Subscriptions
            with get () = subscriptions

          member state.Listener
            with get () = socket

          member state.Dispose() =
            try
              socket.Shutdown(SocketShutdown.Both)
              socket.Close()
            with
              | _ ->
                socket.Dispose()
            signal.Dispose() }

  //   ____                            _   _
  //  / ___|___  _ __  _ __   ___  ___| |_(_) ___  _ __
  // | |   / _ \| '_ \| '_ \ / _ \/ __| __| |/ _ \| '_ \
  // | |__| (_) | | | | | | |  __/ (__| |_| | (_) | | | |
  //  \____\___/|_| |_|_| |_|\___|\___|\__|_|\___/|_| |_|

  module private Connection =

    let sendCallback (result: IAsyncResult) =
      try
        // Retrieve the socket from the state object.
        let handler = result.AsyncState :?> Socket

        // Complete sending the data to the remote device.
        let bytesSent = handler.EndSend(result)

        printfn "Sent %d bytes to client." bytesSent
      with
        | :? ObjectDisposedException -> ()
        | exn ->
          exn.Message
          |> printfn "sendCallback: exn: %s"

    let send id (socket: Socket) subscriptions (data: byte array) =
      try
        socket.BeginSend(
          data,
          0,
          data.Length,
          SocketFlags.None,
          AsyncCallback(sendCallback),
          socket)
        |> ignore
      with
        | :? ObjectDisposedException ->
          id
          |> ServerEvent.Disconnect
          |> Observable.onNext subscriptions
        | exn ->
          exn.Message
          |> printfn "EXN: send: %s"
          id
          |> ServerEvent.Disconnect
          |> Observable.onNext subscriptions

    let private beginReceive (connection: IConnection) callback =
      connection.Socket.BeginReceive(
        connection.Buffer,              // buffer to write to
        0,                              // offset in buffer
        connection.BufferSize,          // size of internal buffer
        SocketFlags.None,               // no flags
        AsyncCallback(callback),        // when done, invoke this callback
        connection)                     // pass-on connection into callback
      |> ignore

    let rec receiveCallback (result: IAsyncResult) =
      let mutable content = String.Empty

      // Retrieve the state object and the handler socket
      // from the asynchronous state object.
      let connection = result.AsyncState :?> IConnection

      try
        // Read data from the client socket.
        let bytesRead = connection.Socket.EndReceive(result)

        if bytesRead > 0 then

          let payload =
            if bytesRead = connection.BufferSize then
              connection.Buffer
            else
              let intermediary = Array.zeroCreate bytesRead
              Array.blit connection.Buffer 0 intermediary 0 bytesRead
              intermediary

          (connection.Id, payload)
          |> ServerEvent.Request
          |> Observable.onNext connection.Subscriptions

          // // Check for end-of-file tag. If it is not there, read
          // // more data.
          // content <- state.Builder.ToString()
          // if (content.IndexOf("<EOF>") > -1) then
          //   // All the data has been read from the
          //   // client. Display it on the console.
          //   printfn "Read %d bytes from socket. \n Data : %s" content.Length content
          //   // Echo the data back to the client.
          // else

        // keep trying to get more
        beginReceive connection receiveCallback
      with
        | :? ObjectDisposedException ->
          connection.Id
          |> ServerEvent.Disconnect
          |> Observable.onNext connection.Subscriptions
        | exn ->
          exn.Message
          |> printfn "EXN: receiveCallback: %s"
          connection.Id
          |> ServerEvent.Disconnect
          |> Observable.onNext connection.Subscriptions

    let rec private checkState (connection: IConnection) =
      async {
        do! Async.Sleep(1000)
        try
          if Socket.isAlive connection.Socket then
            return! checkState connection
          else
            connection.Id
            |> ServerEvent.Disconnect
            |> Observable.onNext connection.Subscriptions
        with
          | _ ->
            connection.Id
            |> ServerEvent.Disconnect
            |> Observable.onNext connection.Subscriptions
      }

    let create (state: Shared.IState) (socket: Socket)  =
      let id = Guid.NewGuid()
      let cts = new CancellationTokenSource()
      let endpoint = socket.RemoteEndPoint :?> IPEndPoint

      let bufSize = 1024
      let buffer = Array.zeroCreate bufSize

      let connection =
        { new IConnection with
            member connection.Socket
              with get () = socket

            member connection.Send (bytes: byte array) =
              send id socket state.Subscriptions bytes

            member connection.Id
              with get () = id

            member connection.IPAddress
              with get () = endpoint.Address

            member connection.Port
              with get () = endpoint.Port

            member connection.Buffer
              with get () = buffer

            member connection.BufferSize
              with get () = bufSize

            member connection.Subscriptions
              with get () = state.Subscriptions

            member connection.Dispose() =
              printfn "disposing %O" id
              try
                cts.Cancel()
                cts.Dispose()
              with
                | _ -> ()
              try
                socket.Shutdown(SocketShutdown.Both)
                socket.Close()
              with
                | _ -> ()
              socket.Dispose() }

      Async.Start(checkState connection, cts.Token)
      beginReceive connection receiveCallback
      connection


  //  ____
  // / ___|  ___ _ ____   _____ _ __
  // \___ \ / _ \ '__\ \ / / _ \ '__|
  //  ___) |  __/ |   \ V /  __/ |
  // |____/ \___|_|    \_/ \___|_|

  module Server =

    let private acceptCallback (state: Shared.IState) (result: IAsyncResult) =
      state.DoneSignal.Set() |> ignore

      let state = result.AsyncState :?> Shared.IState

      try
        let connection =
          result
          |> state.Listener.EndAccept
          |> Connection.create state

        while not (state.Connections.TryAdd(connection.Id, connection)) do
          ignore ()
      with
        | exn ->
          exn.Message
          |> printfn "acceptCallback: %s"

    let private acceptor (state: Shared.IState) () =
      while true do
        state.DoneSignal.Reset() |> ignore
        printfn "Waiting for new connections"
        state.Listener.BeginAccept(AsyncCallback(acceptCallback state), state) |> ignore
        state.DoneSignal.WaitOne() |> ignore

    let private cleanUp (connections: Connections) = function
      | ServerEvent.Disconnect id ->
        match connections.TryRemove id with
        | true, connection -> connection.Dispose()
        | false, _ -> ()
        printfn "current number of connections: %d" connections.Count
      | _ -> ()

    let create (addr: IPAddress) (port: int) =
      let listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
      let endpoint = IPEndPoint(addr, port)

      listener.Bind(endpoint)
      listener.Listen(100)

      let state = Shared.create listener

      let cleaner = Observable.subscribe (cleanUp state.Connections) state.Subscriptions

      let thread = Thread(ThreadStart(acceptor state))
      thread.Start()

      { new IServer with
          member server.Connections
            with get () = state.Connections

          member server.Send (id: Guid) (bytes: byte array) =
            try
              state.Connections.[id].Send bytes
            with
              | exn ->
                exn.Message
                |> printfn "Error in Send: %s"

          member server.Subscribe (callback: ServerEvent -> unit) =
            Observable.subscribe callback state.Subscriptions

          member server.Dispose() =
            cleaner.Dispose()

            for KeyValue(_,connection) in state.Connections.ToArray() do
              connection.Dispose()

            thread.Abort()
            state.Dispose() }
