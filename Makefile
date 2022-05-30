all:

clean:
	rm -rf sources-orig.csv feed.json sources.json sources.json report.json feed/
	rm -rf __pycache__ */__pycache__ .pytest_cache

lint:
	@echo Running pylint...
	@pylint --exit-zero --rcfile=.pylintrc *.py

checkreqs:
	@echo Running pip-missing-reqs...
	@pip-missing-reqs *.py
	@echo Running pip-extra-reqs...
	@pip-extra-reqs --ignore-requirement=urllib3 --ignore-requirement=regex *.py

bandit:
	@echo Running bandit...
	@bandit --quiet -r -x test.py *.py

pytest:
	@echo Running pytest...
	@pytest -s test.py

safety:
	@echo Checking for vulnerable third-party dependencies...
	@safety check --full-report

validjson:
	@mv sources.csv sources-orig.csv ; tail -101 sources-orig.csv > sources.csv
	@echo Checking that csv_to_json.py creates valid JSON files...
	@NO_UPLOAD=1 python csv_to_json.py feed.json
	@mv sources-orig.csv sources.csv
	@json_verify < sources.json
	@json_verify < feed.json
	@echo Checking that sources.json is of the expected size...
	@test `stat -c%s sources.json` -gt 10000
	@echo Checking that feed.json is of the expected size...
	@test `stat -c%s feed.json` -gt 20000
	@echo Checking that feed_processor_multi.py creates a valid JSON file...
	@NO_UPLOAD=1 python feed_processor_multi.py feed
	@json_verify < feed/feed.json
	@echo Checking that the report makes sense...
	@python report-check.py
	@echo Checking that feed/feed.json is of the expected size...
	@test `stat -c%s feed/feed.json` -gt 500000
#	@echo Checking that there are a reasonable number of padded images...
#	@test `ls feed/cache/*.pad | wc -l` -gt 300
#	@echo Checking that all images are padded to the expected size...
#	@test `find feed/cache -type f -name "*.pad" -size -250000c | wc -l` -eq 0
#	@test `find feed/cache -type f -name "*.pad" -size +250000c | wc -l` -eq 0

test: bandit checkreqs lint pytest validjson
