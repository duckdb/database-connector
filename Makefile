.PHONY: all build clean format format-fix format-check test

GENERATOR=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
endif

all: build

clean:
	rm -rf build

format: format-fix

format-fix:
	python scripts/format.py

format-check:
	python scripts/format.py --check

build:
	mkdir -p build/debug
	cd build/debug && cmake -DCMAKE_BUILD_TYPE=Debug $(GENERATOR) ../../test && cmake --build . --config Debug

test: build 
	build/debug/test_database_connection
