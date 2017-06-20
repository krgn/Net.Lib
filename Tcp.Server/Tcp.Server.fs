module Tcp.Server

open Tcp.Lib
open System
open System.Net
open System.Text
open System.Threading

[<EntryPoint>]
let main argv =
  try
    let addr = IPAddress.Parse argv.[0]
    let port = int argv.[1]

    let server = AsyncServer.start addr port

    Console.ReadLine() |> ignore

    server.Dispose()

    0
  with
    | exn ->
      printfn "ex: %s" exn.Message
      1

// [<EntryPoint>]
// let main argv =
//   try
//     let addr = IPAddress.Parse argv.[0]
//     let port = int argv.[1]

//     let server = Server.create addr port

//     let loop (server: IServer) (inbox: MailboxProcessor<Guid * byte[]>) =
//       let rec impl () =
//         async {
//           let! (id, body) = inbox.Receive()
//           body
//           |> Encoding.UTF8.GetString
//           |> printfn "got: %s"
//           server.Send id (Encoding.UTF8.GetBytes "Thanks!")
//           return! impl()
//         }
//       impl()

//     let cts = new CancellationTokenSource()
//     let actor = MailboxProcessor.Start(loop server, cts.Token)

//     server.Subscribe actor.Post |> ignore

//     Console.ReadLine() |> ignore

//     cts.Cancel()
//     server.Dispose()

//     0 // return an integer exit code
//   with
//     | exn ->
//       printfn "%s" exn.Message
//       1
