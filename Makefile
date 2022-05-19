app_name := spark_test
input_zip := input-example.zip

build:
	docker build -t $(app_name) --build-arg INPUT=$(input_zip) .

test:
	docker run --rm -it $(app_name) pytest src/test

run: build test
	docker run $(app_name) python main.py

bash:
	docker run --rm -it $(app_name) /bin/bash
