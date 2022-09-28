test: clean-pyc
	pytest

coverage: clean-pyc
	coverage run --source sqlbucket -m pytest
	coverage report

cov: coverage

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

deploy-loc:
	python3 -m pip install --upgrade build
	python3 -m build

release:
	python3 -m pip install --upgrade twine
	python3 -m twine upload dist/*

empty-dist:
	rm dist/*
