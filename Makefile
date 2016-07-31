default: build

deps:
	go get github.com/dgruber/drmaa
	go get github.com/dgruber/drmaa2

fmt:
	@go fmt src/arg/*.go
	@go fmt src/cmd/*.go
	@go fmt src/cmdutils/*.go
	@go fmt src/drmaautils/*.go
	@go fmt src/process/*.go
	@go fmt src/dmg/*.go
	@go fmt src/mipmaps/*.go

lint:
	@golint src/arg
	@golint src/cmd
	@golint src/cmdutils
	@golint src/drmaautils
	@golint src/process
	@golint src/dmg
	@golint src/mipmaps

test:
	@go test dmg

build-packages:
	@go build arg
	@go build cmdutils
	@go build drmaautils
	@go build dmg
	@go build process
	@go build mipmaps

build: test
	@go build -ldflags "-r ${DRMAA1_LIB_PATH}" src/cmd/dmgservice.go
	@go build -ldflags "-r ${DRMAA1_LIB_PATH}" src/cmd/mipmapservice.go

clean:
	@rm -f dmgservice mipmapservice
