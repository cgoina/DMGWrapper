default: build

deps:
	go get github.com/dgruber/drmaa
	go get github.com/dgruber/drmaa2

fmt:
	@go fmt src/cmd/*.go
	@go fmt src/drmaautils/*.go
	@go fmt src/process/*.go
	@go fmt src/dmg/*.go

lint:
	@golint src/cmd
	@golint src/drmaautils
	@golint src/process
	@golint src/dmg

build:
	@go build process
	@go build drmaautils
	@go build dmg
	@go build src/cmd/dmgservice.go

clean:
	@rm -f dmgservice
