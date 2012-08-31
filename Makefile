MAKEFLAGS = -s

default: darwin

all: darwin linux-386 linux-amd64 manpages gem

manpages:
	cd man; /usr/bin/env rake

gem: fsevents
	mkdir -p rubygem/ext/fsevents-wrapper
	cp build/fsevents-wrapper rubygem/ext/fsevents-wrapper
	cd rubygem; /usr/bin/env rake

fsevents:
	cd ext/fsevents ; $(MAKE)
	mkdir -p build
	cp ext/fsevents/build/Release/fsevents-wrapper build

linux-386: goversion
	cd go/cmd/zeus; CGO_ENABLED=0 GOOS=linux GOARCH=386 $(MAKE) -o zeus-linux-386

linux-amd64: goversion
	cd go/cmd/zeus; CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(MAKE) -o zeus-linux-amd64

darwin: goversion fsevents
	cd go/cmd/zeus; CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(MAKE) -o zeus-darwin

goversion:
	cd go/zeusversion ; /usr/bin/env ruby ./genversion.rb

install: all
	gem install rubygem/pkg/*.gem --no-ri --no-rdoc

clean:
	cd go/cmd/zeus; $(MAKE) clean
	cd man; rake clean
	cd ext/fsevents ; $(MAKE) clean
	cd rubygem ; rake clean
	rm -f go/zeusversion/zeusversion.go
