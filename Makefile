test:
	go mod tidy
	go test -failfast -timeout 10s -race ./...

cover:
	go test -coverprofile=go-cover.profile -timeout 5s ./...
	go tool cover -html=go-cover.profile
	rm go-cover.profile
