module Tcp.Client

open Tcp.Lib
open System
open System.Net
open System.Text
open System.Threading

let handler = function
  | ClientEvent.Connected ->
    printfn "connected!"
  | ClientEvent.Disconnected ->
    printfn "disconected"
  | ClientEvent.Response bytes ->
    bytes
    |> Encoding.UTF8.GetString
    |> printfn "response: %s"

[<EntryPoint>]
let main argv =
  try
    let addr = IPAddress.Parse argv.[0]
    let port = int argv.[1]

    let client = Client.create addr port

    client.Subscribe handler |> ignore

    let mutable run = true
    while run do
      match Console.ReadLine() with
      | "exit" -> run <- false
      | other ->
        other
        |> Encoding.UTF8.GetBytes
        |> client.Send

    client.Dispose()

    0 // return an integer exit code
  with
    | exn ->
      printfn "%s" exn.Message
      1
