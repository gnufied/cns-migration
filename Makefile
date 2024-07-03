CURPATH=$(PWD)
TARGET_DIR=$(CURPATH)/bin

all build:
	CGO_ENABLED=0 GOOS=linux go build  -trimpath -a -o $(TARGET_DIR)/cns-migration cmd/main.go
.PHONY: all build


clean:
	rm -f $(TARGET_DIR)/cns-migration
.PHONY: clean
