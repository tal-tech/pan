BIN := pan

ENV=dev
CUR_PWD=$(shell pwd)

default: all

update-vendor:
	./vendor/vendor.sh $t

all:
	go build -gcflags "-N" -i -o ./bin/$(BIN)
race:
	go build -gcflags "-N" -i -v -o ./bin/$(BIN) -race
w:
	go get github.com/codeskyblue/fswatch && fswatch
dev:
	go build -gcflags "-N -l" -i -x -o ./bin/$(BIN) && cp $(CUR_PWD)/conf/conf_dev.ini $(CUR_PWD)/conf/conf.ini && ./bin/$(BIN)
