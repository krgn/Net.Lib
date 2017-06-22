namespace Net

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

  type Request =
    { RequestId: Guid
      PeerId: Guid
      Body: byte array }

  type Response = Request

  type ClientEvent =
    | Connect
    | Disconnect
    | Response of Response

  type IClientSocket =
    inherit IDisposable
    abstract Id: Guid
    abstract Send: Request -> unit
    abstract Start: unit -> unit
    abstract Subscribe: (ClientEvent -> unit) ->  IDisposable

  type ServerEvent =
    | Connect    of id:Guid * ip:IPAddress * Port:int
    | Disconnect of id:Guid
    | Request    of Request

  type Subscriptions = ConcurrentDictionary<Guid,IObserver<ServerEvent>>

  type IConnection =
    inherit IDisposable
    abstract Socket: Socket
    abstract Send: Response -> unit
    abstract Id: Guid
    abstract IPAddress: IPAddress
    abstract Port: int
    abstract Buffer: byte array
    abstract BufferSize: int
    abstract RequestId: Guid option with get, set
    abstract RequestLength: int with get, set
    abstract Request: ResizeArray<byte>
    abstract SetRequestLength: offset:int -> unit
    abstract SetRequestId: offset:int -> unit
    abstract CopyData: offset:int -> count:int -> unit
    abstract FinishRequest: unit -> unit
    abstract Subscriptions: Subscriptions

  type Connections = ConcurrentDictionary<Guid,IConnection>

  type IServer =
    inherit IDisposable
    abstract Connections: Connections
    abstract Send: Response -> unit
    abstract Subscribe: (ServerEvent -> unit) ->  IDisposable

  type PubSubEvent =
    | Request of Guid * byte array

  type IPubSub =
    inherit IDisposable
    abstract Send: byte array -> unit
    abstract Subscribe: (PubSubEvent -> unit) -> IDisposable


  [<Literal>]
  let MSG_LENGTH_OFFSET = 4             // int32 has 4 bytes

  [<Literal>]
  let ID_LENGTH_OFFSET = 16             // Guid has 16 bytes

  [<Literal>]
  let HEADER_OFFSET = 20                // total offset, e.g. MSG_LENGTH_OFFSET + ID_LENGTH_OFFSET

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

    let rec checkState<'t> (socket: Socket)
                           (subscriptions: ConcurrentDictionary<Guid,IObserver<'t>>)
                           (ev: 't) =
      async {
        do! Async.Sleep(1000)
        try
          if isAlive socket then
            return! checkState socket subscriptions ev
          else
            Observable.onNext subscriptions ev
        with
          | _ -> Observable.onNext subscriptions ev
      }


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
      abstract ResponseId: Guid option with get, set
      abstract ResponseLength: int with get, set
      abstract Response: ResizeArray<byte>
      abstract SetResponseId: offset:int -> unit
      abstract SetResponseLength: offset:int -> unit
      abstract CopyData: offset:int -> count:int -> unit
      abstract FinishResponse: unit -> unit
      abstract Subscriptions: Subscriptions

    let private connectCallback (ar: IAsyncResult) =
      try
        let state = ar.AsyncState :?> IState

        // Complete the connection.
        state.Socket.EndConnect(ar)

        ClientEvent.Connect
        |> Observable.onNext state.Subscriptions

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
          // this is a fresh response, so we start of nice and neat
          if state.Response.Count = 0 && Option.isNone state.ResponseId then
            state.SetResponseLength 0
            state.SetResponseId MSG_LENGTH_OFFSET
            state.CopyData HEADER_OFFSET (bytesRead - HEADER_OFFSET)

          elif state.Response.Count < state.ResponseLength then
            let required = state.ResponseLength - state.Response.Count
            if required >= state.BufferSize && bytesRead = state.BufferSize then
              // just add the entire current buffer
              state.CopyData 0 state.BufferSize
            elif required <= bytesRead then
              state.CopyData 0 required
              if state.Response.Count = state.ResponseLength then
                state.FinishResponse()
              let remaining = bytesRead - required
              if remaining > 0 && remaining >= HEADER_OFFSET then
                state.SetResponseLength required
                state.SetResponseId (required + MSG_LENGTH_OFFSET)
                state.CopyData (required + HEADER_OFFSET) remaining
            else state.CopyData 0 bytesRead

          if state.ResponseLength = state.Response.Count then
            state.FinishResponse()

        beginReceive state
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let private beginReceive (state: IState) =
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

    let private send (state: IState) (request: Request) =
      try
        let header =
          Array.append
            (BitConverter.GetBytes request.Body.Length)
            (request.RequestId.ToByteArray())

        let payload = Array.append header request.Body

        // Begin sending the data to the remote device.
        state.Socket.BeginSend(
          payload,
          0,
          payload.Length,
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

      let cts = new CancellationTokenSource()
      let endpoint = IPEndPoint(addr, port)
      let client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)

      let state =
        let bufSize = 256
        let buffer = Array.zeroCreate bufSize
        let connected = new ManualResetEvent(false)
        let sent = new ManualResetEvent(false)
        let mutable responseId = None
        let mutable responseLength = 0
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

            member state.ResponseId
              with get () = responseId
               and set id = responseId <- id

            member state.ResponseLength
              with get ()  = responseLength
               and set len = responseLength <- len

            member state.SetResponseLength(offset: int) =
              responseLength <- BitConverter.ToInt32(buffer, offset)

            member state.SetResponseId(offset: int) =
              responseId <-
                let intermediary = Array.zeroCreate ID_LENGTH_OFFSET
                Array.blit buffer offset intermediary 0 ID_LENGTH_OFFSET
                intermediary
                |> Guid
                |> Some

            member state.CopyData (offset: int) (count: int) =
              let intermediary = Array.zeroCreate count
              Array.blit buffer offset intermediary 0 count
              response.AddRange intermediary

            member state.FinishResponse() =
              match responseId with
              | Some guid ->
                { PeerId = id
                  RequestId = guid
                  Body = response.ToArray() }
                |> ClientEvent.Response
                |> Observable.onNext subscriptions
                responseLength <- 0
                responseId <- None
                response.Clear()
              | None -> ()

            member state.Subscriptions
              with get () = subscriptions

            member state.Dispose() =
              Socket.dispose client
          }

      let checker = Socket.checkState client state.Subscriptions ClientEvent.Disconnect

      { new IClientSocket with
          member socket.Send(request: Request) =
            // Send test data to the remote device.
            send state request

          member socket.Start() =
            beginConnect state
            Async.Start(checker, cts.Token)
            beginReceive state

          member socket.Id
            with get () = id

          member socket.Subscribe (callback: ClientEvent -> unit) =
            Observable.subscribe callback state.Subscriptions

          member socket.Dispose () =
            cts.Cancel()
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

    let send (response: Response) (socket: Socket) id subscriptions =
      try
        let header =
          Array.append
            (BitConverter.GetBytes response.Body.Length)
            (response.RequestId.ToByteArray())
        let payload = Array.append header response.Body
        socket.BeginSend(
          payload,
          0,
          payload.Length,
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

          printfn "received %d bytes" bytesRead

          // this is a new request, so we determine the length of the request
          // and the request id
          if connection.Request.Count = 0 && Option.isNone connection.RequestId then
            connection.SetRequestLength (offset = 0)
            connection.SetRequestId MSG_LENGTH_OFFSET
            connection.CopyData HEADER_OFFSET (bytesRead - HEADER_OFFSET)
          elif connection.Request.Count < connection.RequestLength then
            // we are currently working on a request, so we add to request array whatever data is
            // available
            let required = connection.RequestLength - connection.Request.Count
            if required >= connection.BufferSize && bytesRead = connection.BufferSize then
              // just add the entire current buffer
              connection.CopyData 0 connection.BufferSize
            elif required <= bytesRead then
              connection.CopyData 0 required
              if connection.Request.Count = connection.RequestLength then
                connection.FinishRequest()
              let remaining = bytesRead - required
              if remaining > 0 && remaining >= HEADER_OFFSET then
                connection.SetRequestLength required
                connection.SetRequestId (required + MSG_LENGTH_OFFSET)
                connection.CopyData (required + HEADER_OFFSET) remaining
            else connection.CopyData 0 bytesRead

          if connection.RequestLength = connection.Request.Count then
            connection.FinishRequest()

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

    let create (state: Shared.IState) (socket: Socket)  =
      let id = Guid.NewGuid()
      let cts = new CancellationTokenSource()
      let endpoint = socket.RemoteEndPoint :?> IPEndPoint

      let bufSize = 1024
      let buffer = Array.zeroCreate bufSize

      let mutable requestLength = 0
      let mutable requestId = None
      let request = ResizeArray()

      let connection =
        { new IConnection with
            member connection.Socket
              with get () = socket

            member connection.Send (response: Response) =
              send response socket id state.Subscriptions

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

            member connection.RequestId
              with get () = requestId
               and set id = requestId <- id

            member connection.RequestLength
              with get () = requestLength
               and set len = requestLength <- len

            member connection.Request
              with get () = request

            member connection.SetRequestLength(offset: int) =
              requestLength <- BitConverter.ToInt32(buffer, offset)

            member connection.SetRequestId(offset: int) =
              requestId <-
                let intermediary = Array.zeroCreate ID_LENGTH_OFFSET
                Array.blit buffer offset intermediary 0 ID_LENGTH_OFFSET
                intermediary
                |> Guid
                |> Some

            member connection.CopyData (offset: int) (count: int) =
              // read the rest into the ResizeArray
              let intermediary = Array.zeroCreate count
              Array.blit connection.Buffer offset intermediary 0 count
              connection.Request.AddRange intermediary

            member connection.FinishRequest() =
              match requestId with
              | Some guid ->
                { PeerId = id
                  RequestId = guid
                  Body = request.ToArray() }
                |> ServerEvent.Request
                |> Observable.onNext connection.Subscriptions
                requestLength <- 0
                requestId <- None
                request.Clear()
              | None -> ()

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

      let checker =
        Socket.checkState
          connection.Socket
          connection.Subscriptions
          (ServerEvent.Disconnect connection.Id)

      Async.Start(checker, cts.Token)
      beginReceive connection receiveCallback
      connection


  //  ____
  // / ___|  ___ _ ____   _____ _ __
  // \___ \ / _ \ '__\ \ / / _ \ '__|
  //  ___) |  __/ |   \ V /  __/ |
  // |____/ \___|_|    \_/ \___|_|

  module Server =

    let private acceptCallback =
      AsyncCallback(fun (result: IAsyncResult) ->
        let state = result.AsyncState :?> Shared.IState
        state.DoneSignal.Set() |> ignore

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
            |> printfn "acceptCallback: %s")

    let private acceptor (state: Shared.IState) () =
      while true do
        state.DoneSignal.Reset() |> ignore
        printfn "Waiting for new connections"
        state.Listener.BeginAccept(acceptCallback, state) |> ignore
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

          member server.Send (response: Response) =
            try
              state.Connections.[response.PeerId].Send response
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

  //  ____        _    ____        _
  // |  _ \ _   _| |__/ ___| _   _| |__
  // | |_) | | | | '_ \___ \| | | | '_ \
  // |  __/| |_| | |_) |__) | |_| | |_) |
  // |_|    \__,_|_.__/____/ \__,_|_.__/

  module rec PubSub =

    let MultiCastAddress = IPAddress.Parse "239.0.0.222"

    type private IState =
      abstract Id: Guid
      abstract LocalEndPoint: IPEndPoint
      abstract RemoteEndPoint: IPEndPoint
      abstract Client: UdpClient
      abstract Subscriptions: ConcurrentDictionary<Guid,IObserver<PubSubEvent>>

    let private receiveCallback (ar: IAsyncResult) =
      let state = ar.AsyncState :?> IState
      let raw = state.Client.EndReceive(ar, &state.LocalEndPoint)

      let guid =
        let intermediate = Array.zeroCreate 16
        Array.blit raw 0 intermediate 0 16
        Guid intermediate

      if guid <> state.Id then
        let payload =
          let intermedate = raw.Length - 16 |> Array.zeroCreate
          Array.blit raw 16 intermedate 0 (raw.Length - 16)
          intermedate

        (guid, payload)
        |> PubSubEvent.Request
        |> Observable.onNext state.Subscriptions

      beginReceive state

    let private beginReceive (state: IState) =
      state.Client.BeginReceive(AsyncCallback(receiveCallback), state)
      |> ignore

    let private sendCallback (ar: IAsyncResult) =
      try
        let state = ar.AsyncState :?> IState
        state.Client.EndSend(ar)
        |> printfn "Bytes sent: %d"
      with
        | exn ->
          exn.Message
          |> printfn "exn: %s"

    let private beginSend (state: IState) (data: byte array) =
      let payload = Array.append (state.Id.ToByteArray()) data
      state.Client.BeginSend(
        payload,
        payload.Length,
        state.RemoteEndPoint,
        AsyncCallback(sendCallback),
        state)
      |> ignore

    let create (multicastAddress: IPAddress) (port: int) =
      let id = Guid.NewGuid()

      let subscriptions = ConcurrentDictionary<Guid,IObserver<PubSubEvent>>()

      let client = new UdpClient()
      client.ExclusiveAddressUse <- false

      let remoteEp = IPEndPoint(multicastAddress, port)
      let localEp = IPEndPoint(IPAddress.Any, port)

      client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true)
      client.Client.Bind(localEp)
      client.JoinMulticastGroup(multicastAddress)

      let state =
        { new IState with
            member state.Id
              with get () = id
            member state.LocalEndPoint
              with get () = localEp
            member state.RemoteEndPoint
              with get () = remoteEp
            member state.Client
              with get () = client
            member state.Subscriptions
              with get () = subscriptions }

      beginReceive state

      { new IPubSub with
          member pubsub.Send(bytes: byte array) =
            beginSend state bytes

          member pubsub.Subscribe (callback: PubSubEvent -> unit) =
            Observable.subscribe callback subscriptions

          member pubsub.Dispose () =
            client.Dispose() }
