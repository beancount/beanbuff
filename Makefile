#!/usr/bin/env make
#
# Run tests like this:
#
#    make BEANMEAT=/path/to/root/of/downloaded/files

IMPORTERS =					\
	ameritrade/thinkorswim_csv.py		\
	ameritrade/thinkorswim_forex.py		\
	tastyworks/tastyworks_csv.py		\
	ibkr/ibkr_xls.py			\
	ibkr/ibkr_flex_reports_csv.py		\
	oanda/oanda_csv.py			\
	oanda/oanda_pdf.py			\
	vanguard/vanguard_csv.py		\
	vanguard/vanguard_ofx.py		\
	vanguard/vanguard_pdf.py		\
	fidelity/fidelity_pdf.py		\
	lendingclub/lendingclub_pdf.py

test: $(IMPORTERS:.py=.test)

$(IMPORTERS:.py=.test) : %.test : beanbuff/%.py
	python3 $< test $(BEANMEAT)/$(basename $@)


chains:
	python3 -m beanbuff.data.consolidated $(HOME) --inactive

buff:
	FLASK_ENV=development FLASK_APP=beanbuff.scripts.buff flask run

tobean:
	@python3 -m beanbuff.data.tobean $(HOME)/tobean.json $(L) $(HOME)/tasty*1887*.csv | tee $(HOME)/x1887.beancount
	@python3 -m beanbuff.data.tobean $(HOME)/tobean.json $(L) $(HOME)/tasty*2003*.csv | tee $(HOME)/x2003.beancount
	@python3 -m beanbuff.data.tobean $(HOME)/tobean.json $(L) $(HOME)/*Statement.csv | tee $(HOME)/x9627.beancount
