module Tcp.Pub

open Tcp.Lib
open System
open System.Net
open System.Text
open System.Threading

[<EntryPoint>]
let main argv =
  let addr = IPAddress.Parse argv.[0]
  let port = int argv.[1]

  PubSub.sender addr port |> ignore
  0
