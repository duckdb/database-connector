.PHONY: all build build_threads clean format format-fix format-check test test_threads

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
	cd build/debug && \
		cmake -DCMAKE_BUILD_TYPE=Debug $(GENERATOR) ../../test && \
		cmake --build . --config Debug

build_threads:
	mkdir -p build/debug
	cd build/debug && \
		cmake -DCMAKE_BUILD_TYPE=Debug $(GENERATOR) -DENABLE_THREAD_SANITIZER=ON ../../test && \
		cmake --build . --config Debug

test: build 
	build/debug/test_database_connection

test_threads: build_threads
	build/debug/test_database_connection
