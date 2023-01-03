all:

clean:
	rm -rf sources-orig.csv feed.json sources.json sources.json report.json feed/
	rm -rf __pycache__ */__pycache__ .pytest_cache

pytest:
	echo Running pytest...
	pytest -s test.py

validjson:
	mv sources.csv sources-orig.csv ; head -10 sources-orig.csv > sources.csv
	echo Checking that csv_to_json.py creates valid JSON files...
	NO_UPLOAD=1 NO_DOWNLOAD=1 PYTHONPATH=. python csv_to_json.py feed.json
	mv sources-orig.csv sources.csv
	json_verify < sources.json
	json_verify < feed.json
	echo Checking that sources.json is of the expected size...
	test `stat -c%s sources.json` -gt 10
	echo Checking that feed.json is of the expected size...
	test `stat -c%s feed.json` -gt 10
	echo Checking that feed_processor_multi.py creates a valid JSON file...
	NO_UPLOAD=1 NO_DOWNLOAD=1 PYTHONPATH=. python feed_processor_multi.py feed
	json_verify < feed/feed.json
	echo Checking that the report makes sense...
	PYTHONPATH=. python report-check.py
	echo Checking that feed/feed.json is of the expected size...
	test `stat -c%s feed/feed.json` -gt 1000

test: pytest validjson
