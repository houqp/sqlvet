test:
	go mod tidy
	go test -failfast -timeout 10s -race ./...

cover:
	go test -coverprofile=go-cover.profile -timeout 5s ./...
	go tool cover -html=go-cover.profile
	rm go-cover.profile

# release:
# 	gox -output "dist/sqlvet_{{.OS}}_{{.Arch}}" \
# 		-osarch="linux/amd64" \
# 		-osarch="windows/amd64" \
# 		-osarch="freebsd/amd64" \
# 		-osarch="openbsd/amd64" \
# 		-osarch="netbsd/amd64" \
# 		-osarch="darwin/amd64" \
# 		./...
# 	ghr -t ${GITHUB_TOKEN} \
# 		-u houqp \
# 		--draft \
# 		-r sqlvet \
# 		-c `git rev-parse HEAD` \
# 		--replace `git describe --tags` \
# 		./dist/
