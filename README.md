## Socket Examples

This repository contains some examples for working with asynchronous
sockets in [FSharp](http://fsharp.org). 

## Building

To build, depending on your platform, invoke:

```shell
# Windows

build.cmd

# *nix

build.sh
```

## Trying things out

First, navigate to the `build/` directory.

### Tcp Server/Client

To start the server:

```shell
mono ./Server.exe 127.0.0.1 9999
```

Next, start the client: 

```shell
mono ./Client.exe 127.0.0.1 9999
```

Once the client is connected, start typing things into the terminal and hit Enter.

### PubSub

Start multiple `PubSub` client like this:

```shell
mono ./PubSub.exe 2222
```

Now, start typing into the terminal windows.
