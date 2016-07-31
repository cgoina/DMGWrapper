default: build

deps:
	go get github.com/dgruber/drmaa
	go get github.com/dgruber/drmaa2

fmt:
	@go fmt src/cmd/*.go
	@go fmt src/drmaautils/*.go
	@go fmt src/process/*.go
	@go fmt src/dmg/*.go
	@go fmt src/mipmaps/*.go

lint:
	@golint src/cmd
	@golint src/drmaautils
	@golint src/process
	@golint src/dmg
	@golint src/mipmaps

test:
	@go test dmg

build: test
	@go build process
	@go build drmaautils
	@go build dmg
	@go build mipmaps
	@go build -ldflags "-r ${DRMAA1_LIB_PATH}" src/cmd/dmgservice.go

clean:
	@rm -f dmgservice
