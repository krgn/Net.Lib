all:
	@./build.sh

paket.install:
	@mono .paket/paket.exe install

paket.restore:
	@mono .paket/paket.exe restore

clean:
	@git clean -fdX
