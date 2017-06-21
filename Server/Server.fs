module Server

open Net.Lib
open System
open System.Net
open System.Text
open System.Threading

[<EntryPoint>]
let main argv =
  try
    let addr = IPAddress.Parse argv.[0]
    let port = int argv.[1]

    let server = Server.create addr port

    let loop (server: IServer) (inbox: MailboxProcessor<ServerEvent>) =
      let rec impl () =
        async {
          let! msg = inbox.Receive()
          match msg with
          | ServerEvent.Connect(id, ip, port) ->
            printfn "new connection %O from %O:%d" id ip port
          | ServerEvent.Disconnect id ->
            printfn "connection %O closed" id
          | ServerEvent.Request request ->
            request.Body
            |> Encoding.UTF8.GetString
            |> printfn "requestid: %O payload: %s" request.RequestId
            server.Send { request with Body = Encoding.UTF8.GetBytes "Thanks!" }
          return! impl()
        }
      impl()

    let cts = new CancellationTokenSource()
    let actor = MailboxProcessor.Start(loop server, cts.Token)

    server.Subscribe actor.Post |> ignore

    Console.ReadLine() |> ignore

    cts.Cancel()
    server.Dispose()

    0 // return an integer exit code
  with
    | exn ->
      printfn "%s" exn.Message
      1
