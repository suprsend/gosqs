pretest:
	@docker run --rm -p 4100:4100 -v $(CURDIR)/emulator/sqsconf.yaml:/app/conf/goaws.yaml admiralpiett/goaws 

test:
	@go test ./...

