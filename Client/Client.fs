module Client

open Net.Lib
open System
open System.Net
open System.Text
open System.Threading

let handler = function
  | ClientEvent.Connect ->
    printfn "connected!"
  | ClientEvent.Disconnect ->
    printfn "disconected"
  | ClientEvent.Response response ->
    response.Body
    |> Encoding.UTF8.GetString
    |> printfn "requestid: %O response: %s" response.RequestId

[<EntryPoint>]
let main argv =
  try
    let addr = IPAddress.Parse argv.[0]
    let port = int argv.[1]

    let client = Client.create addr port
    client.Subscribe handler |> ignore

    client.Start()

    let mutable run = true
    while run do
      match Console.ReadLine() with
      | "exit" -> run <- false
      | other ->
        other
        |> Encoding.UTF8.GetBytes
        |> fun body -> { RequestId = Guid.NewGuid(); PeerId = client.Id; Body = body }
        |> fun req -> printfn "request: %O" req.RequestId; req
        |> client.Send

    client.Dispose()

    0 // return an integer exit code
  with
    | exn ->
      printfn "%s" exn.Message
      1
