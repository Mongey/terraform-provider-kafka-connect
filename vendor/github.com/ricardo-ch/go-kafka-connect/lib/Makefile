.PHONY: install
install:
	go get -u github.com/golang/dep/cmd/dep
	dep ensure -v

.PHONY: test-integration
test-integration:
	go test -v `go list ./... | grep -v /vendor/` -tags=integration

