module Tcp.Server

open Tcp.Lib
open System
open System.Net

[<EntryPoint>]
let main argv =
  try
    let addr = IPAddress.Parse argv.[0]
    let port = int argv.[1]

    let server = Server.create addr port

    Console.ReadLine() |> ignore

    server.Dispose()

    0 // return an integer exit code
  with
    | exn ->
      printfn "%s" exn.Message
      1
